#!/usr/bin/env bash

#
# map-reduce tests
#

# un-comment this to run the tests with the Go race detector.
# RACE=-race

# 检测 macOS 环境并禁用有问题的竞态检测
if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

# 静默模式函数实现
ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}

# 超时命令检测
TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1 # 先尝试使用系统自带的timeout命令（常见于 GNU/Linux 系统）
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1 # 如果没有timeout，则尝试使用gtimeout（常见于 macOS 系统，由 coreutils 包提供）
  then
    TIMEOUT=gtimeout
  else # 如果都没有，则将TIMEOUT变量置空，表示不使用超时功能
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
# 超时参数设置
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s " # 当运行超过45秒时，发送 SIGTERM 信号请求终止,若 2 秒后仍未终止，发送 SIGKILL 强制终止。
fi

# 创建一个新的临时目录用于测试
# run the test in a fresh sub-directory.
rm -rf mr-tmp        # 删除已存在的mr-tmp目录（递归删除且不提示）
mkdir mr-tmp || exit 1  # 创建mr-tmp目录，若失败则退出脚本（返回状态码1）
cd mr-tmp || exit 1     # 进入mr-tmp目录，若失败则退出脚本
rm -f mr-*             # 删除mr-tmp目录下所有以"mr-"开头的文件（强制删除不提示）

# 编译 MapReduce 框架的各个组件
# make sure software is freshly built.
# 清理旧的编译文件
(cd ../../mrapps && go clean)
(cd .. && go clean)
# 编译mrapps目录下的所有插件（以.go结尾）
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
# 编译主程序
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
# 执行顺序版 MapReduce 程序并验证结果

../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt # 对输出结果排序并保存为正确答案
rm -f mr-out* # 清理中间输出文件
echo '***' Starting wc test.

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt & # 启动协调者（Coordinator）进程并设置超时限制
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) & # 启动工作者（Worker）进程并设置超时限制
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/wc.so) &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all # 合并并处理所有Worker输出文件。sort mr-out*：将所有mr-out-*文件（Worker 输出）按字典序排序后合并。 grep . 过滤掉空行（确保结果中不含空白行）
if cmp mr-wc-all mr-correct-wc.txt # 对比结果与基准文件
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/indexer.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/mtiming.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait


#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/rtiming.so  &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt  &
sleep 1

maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so &
maybe_quiet $TIMEOUT ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -f mr-*

echo '***' Starting early exit test.

DF=anydone$$
rm -f $DF

(maybe_quiet $TIMEOUT ../mrcoordinator ../pg*txt; touch $DF) &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &
(maybe_quiet $TIMEOUT ../mrworker ../../mrapps/early_exit.so; touch $DF) &

# wait for any of the coord or workers to exit.
# `jobs` ensures that any completed old processes from other tests
# are not waited upon.
jobs &> /dev/null
if [[ "$OSTYPE" = "darwin"* ]]
then
  # bash on the Mac doesn't have wait -n
  while [ ! -e $DF ]
  do
    sleep 0.2
  done
else
  # the -n causes wait to wait for just one child process,
  # rather than waiting for all to finish.
  wait -n
fi

rm -f $DF

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
((maybe_quiet $TIMEOUT2 ../mrcoordinator ../pg*txt); touch mr-done ) &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  maybe_quiet $TIMEOUT2 ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi

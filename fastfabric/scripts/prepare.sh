#!/bin/bash

NAME=$1

AWK_4='NR%2{prev=$0} NR%2==0{printf "%s\t%s\n",prev,$0}'
AWK_6='NR%3==1{prev1=$0} NR%3==2{prev2=$0} NR%3==0{printf "%s\t%s\t%s\n",prev1,prev2,$0}'
TARGET=
FILES=

cd /home/jjoriping/Power-Fabric

# NAME 유효성 검사
for v in $(ls inputs); do
  if [ "$v" == "$NAME" ]; then
    echo "[INF] $NAME 폴더 확인"
    TARGET="inputs/$NAME"
    FILES=$(ls $TARGET)
  fi
done
if [ -z "$FILES" ]; then
  echo "[ERR] 올바른 NAME을 입력해 주세요."
  exit 1
fi

# 전처리
# if [ ! -e "$TARGET.send2.bundled" ]; then
#   sort -R --parallel=40 "$TARGET.merged" > "$TARGET.send2.bundled"
# fi
# if [ ! -e "$TARGET.sendx.bundled" ]; then
  echo "[INF] 전처리 시작"
  NO_SHUFFLE=0
  for f in $FILES; do
    if [[ "$f" == *.conf ]]; then
      if [ "$f" == "no-shuffle.conf" ]; then
        echo "[INF] NO_SHUFFLE 감지"
        NO_SHUFFLE=1
      fi
      continue
    fi
    cat "$TARGET/$f" >> "$TARGET.merged"
  done
  if [ "$NO_SHUFFLE" -eq 0 ]; then
    sort -R --parallel=40 "$TARGET.merged" > "$TARGET.sendx.bundled"
  else
    cp "$TARGET.merged" "$TARGET.sendx.bundled"
  fi
# fi
# if [ ! -e "$TARGET.send4.bundled" ]; then
#   cat "$TARGET.send2.bundled" | awk "$AWK_4" | sort -R --parallel=40 > "$TARGET.send4.bundled"
# fi
# if [ ! -e "$TARGET.send6.bundled" ]; then
#   cat "$TARGET.send2.bundled" | awk "$AWK_6" | sort -R --parallel=40 > "$TARGET.send6.bundled"
# fi
if [ -e "$TARGET.merged" ]; then
  rm "$TARGET.merged"
fi
# ls -l fabric-benchmarking-inputs/*.bundled
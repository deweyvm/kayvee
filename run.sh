#!/usr/bin/env bash
EXE=dist/build/kayvee/kayvee

cabal build &&\
$EXE
if [[ $? -ne 0 ]] ; then
    echo "Failure: exe"
else
    echo "Success: exe"
fi

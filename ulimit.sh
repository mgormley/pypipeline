#!/usr/bin/bash

# Values are in 1024-byte increments

ulimit -v 2000000
echo $*
$*
EXIT=$?
if [[ $EXIT != 0 ]] ; 
then
    echo Error $EXIT
else
    echo Success $EXIT
fi
exit $EXIT

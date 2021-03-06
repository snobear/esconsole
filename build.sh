#!/bin/bash
set -x

curdir=$(dirname $0)

outdir=pkg
if [[ $# -ne 0 ]] ; then
    outdir=$1
fi

if [[ $2 != "--no-delete" ]] ; then
    rm -rf $outdir
fi

mkdir $outdir
mkdir -p $outdir/usr/local/bin

esconsole_script=$outdir/usr/local/bin/esconsole
cp $curdir/esconsole-launch.sh $outdir/usr/local/bin/esconsole
chmod 755 $outdir/usr/local/bin/esconsole


optdir=$outdir/opt/esconsole
venvdir=$optdir/venv

mkdir -p $optdir


(cd $optdir ; virtualenv venv)
. $venvdir/bin/activate

pip install -r $curdir/requirements.txt


cp $curdir/LICENSE.txt $optdir
cp -r $curdir/esconsole $optdir

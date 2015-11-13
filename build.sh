#!/bin/bash

cd $(dirname $0)

outdir=pkg

rm -rf $outdir

mkdir $outdir
mkdir -p $outdir/usr/local/bin

esconsole_script=$outdir/usr/local/bin/esconsole
cp esconsole-launch.sh $outdir/usr/local/bin/esconsole
chmod 755 $outdir/usr/local/bin/esconsole


optdir=$outdir/opt/esconsole
venvdir=$optdir/venv

mkdir -p $optdir


virtualenv $venvdir
. $venvdir/bin/activate

pip install -r requirements.txt


cp LICENSE.txt $optdir
cp -r esconsole $optdir

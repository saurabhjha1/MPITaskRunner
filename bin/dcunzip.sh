src=$1
dest=$2
fname=`echo $src|sed s:/:_:g`
echo $dest
echo $src
cp $src $dest #"/"$fname
#gunzip $dest"/"$fname
touch $dest"/"$fname".complete"

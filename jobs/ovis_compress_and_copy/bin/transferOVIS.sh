src=$1
dest=$2
fname=`basename $src`
echo $dest
echo $src
if [[ $src =~ \.t?gz$ ]]; then
	cp $src $dest
else
   env GZIP=-9 tar -cvzf  $dest"/"$fname".tgz" $src
fi
touch $dest"/"$fname".complete"

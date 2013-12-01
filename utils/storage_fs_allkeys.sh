#/bin/sh

if [ "X$1" == "X" ]; then
    echo "Usage : $0 <storage_dir>"
    exit
fi

for i in `find $1 -type f`; do
    name=`basename $i`
    perl -e '$a = join("", map { chr(hex($_)) } unpack("(A2)*", "$ARGV[0]")); print "$a\n"' $name 2>/dev/null
done

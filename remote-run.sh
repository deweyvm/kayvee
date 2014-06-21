NAME=kayvee
HOST=olivine
pushd ../ &> /dev/null &&\
echo "Remove local $NAME.zip" &&\
rm -rf $NAME.zip &&\
echo "Zip new files" &&\
zip -r  --exclude=*.git* --exclude=*.cabal-sandbox* --exclude=*dist* --exclude=*cabal.sandbox.config* $NAME.zip $NAME &> /dev/null &&\
echo "Delete $HOST zip" &&\
ssh $HOST "rm -rf ~/$NAME.zip ~/$NAME" &&\
echo "Upload to $HOST" &&\
scp $NAME.zip $HOST:. &&\
echo "Run program" &&\
ssh -t $HOST "unzip -qq $NAME.zip && cd $NAME && cabal configure && sh run.sh"
if [[ $? -ne 0 ]] ; then
    echo "Failure-------------------------------------------------------------------------"
else
    echo "Success+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    time=`date`
    echo "Synced at $time"
fi
popd &> /dev/null

# run on an other instance

# Clone Repo: only necessary for the first time, then with pull
#git clone https://github.com/davidedironza/gcp_test.git
#echo "git repo cloned"

# separate commands in on-liner with &&, not needed if separated by rows

export INSTANCE_NAME="instance-bindexis"

gcloud compute instances start $INSTANCE_NAME --zone europe-west6-a

sleep 10

gcloud compute ssh $INSTANCE_NAME --zone europe-west6-a --command '
cd gcp_test
git pull https://github.com/davidedironza/gcp_test.git
echo "git repo pulled"
cd
'

sleep 10

gcloud compute ssh $INSTANCE_NAME --zone europe-west6-a --command '
cd gcp_test/source
python2 bindexis_dataload.py
OK="$?"
echo $OK
if [ "$OK" -eq 0 ]
then
    python3 bindexis_trigger.py
    echo "Bindexis Trigger executed"
else
    echo "Fehler"
fi
'

gcloud compute instances stop $INSTANCE_NAME --zone europe-west6-a

# run on an other instance

# Clone Repo: only necessary for the first time, then with pull
#git clone https://github.com/davidedironza/gcp_test.git
#echo "git repo cloned"

# separate commands in on-liner with &&, not needed if separated by rows

export INSTANCE_NAME="instance-bindexis"

gcloud compute instances start $INSTANCE_NAME

gcloud compute ssh $INSTANCE_NAME --zone europe-west6-a --command '
cd gcp_test/source
python3 python_script_write1.py
echo "Write 1 executed"
python3 python_script_write2.py
echo "Write 2 executed"
'

gcloud compute instances stop $INSTANCE_NAME

# run on an other instance

# Clone Repo: only necessary for the first time, then with pull
#git clone https://github.com/davidedironza/gcp_test.git
#echo "git repo cloned"

gcloud compute ssh instance-5 --zone europe-west6-a -- 'cd gcp_test/source && cd python3 python_script_write1.py'

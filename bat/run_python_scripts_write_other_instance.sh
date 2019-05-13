# run on an other instance

# Clone Repo: only necessary for the first time, then with pull
#git clone https://github.com/davidedironza/gcp_test.git
#echo "git repo cloned"

gcloud compute ssh instance-5 --zone europe-west6-a

cd gcp_test
#git pull

cd source

python3 python_script_write1.py
echo "Write 1 executed"

python3 python_script_write2.py
echo "Write 2 executed"

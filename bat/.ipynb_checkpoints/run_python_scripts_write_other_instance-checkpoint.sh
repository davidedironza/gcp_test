# run on an other instance

# Clone Repo: only necessary for the first time, then with pull
#git clone https://github.com/davidedironza/gcp_test.git
#echo "git repo cloned"

# separate commands in on-liner with &&, not needed if separated by rows

gcloud compute ssh instance-5 --zone europe-west6-a --command '
cd gcp_test/source
python3 python_script_write1.py
OK="$?"
echo $OK
if [ "$OK" -eq 0 ]
then
    python3 python_script_write2.py
    echo "Write 2 executed"
else
    echo "Fehler"
fi
'

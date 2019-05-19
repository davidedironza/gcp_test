# run directly on an instance

# Clone Repo: only necessary for the first time, then with pull
#git clone https://github.com/davidedironza/gcp_test.git
#echo "git repo cloned"

cd
cd gcp_test
#git pull

cd source

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

rm -rf mr-tmp/*
ps -ef | grep  test-mr.sh | awk '{print $2}' | xargs kill -9

echo "clone the base image repo"
git clone git@github.com:22Acacia/headspring.git $HOME/headspring
ret_var=$?

sudo service docker start
ret_var=$?
if [ $ret_var -ne 0 ]; then
  echo "starting the docker service failed  abort"
  exit $ret_var
fi

if [ $ret_var -eq 0 ]; then
  echo "Build base image"
  cd $HOME/headspring
  sudo docker build -t acacia acacia/
  ret_var=$?
fi

if [ $ret_var -eq 0 ]; then
  echo "Build sink examples image"
  cd $HOME/$CIRCLE_PROJECT_REPONAME/
  sudo docker build -t gcr.io/$GOOGLE_PROJECT/sink-examples-master .
  ret_var=$?
fi

exit $ret_val

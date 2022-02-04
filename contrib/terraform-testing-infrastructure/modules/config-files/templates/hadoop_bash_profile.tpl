# .bash_profile
# Get the aliases and functions
if [ -f ~/.bashrc ]; then
  source ~/.bashrc
fi
# User specific environment and startup programs
PATH=$PATH:$HOME/.local/bin:$HOME/bin:$ZOOKEEPER_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$ACCUMULO_HOME/bin
export PATH
ulimit -n 32768

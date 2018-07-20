# internal functions just for uno cluster control

UNO_HOME=/home/ubuntu/git/uno
UNO=$UNO_HOME/bin/uno

function get_ah {
  echo "$($UNO env | grep ACCUMULO_HOME | sed 's/export ACCUMULO_HOME=//' | sed 's/"//g')"
}


# functions required for accumulo testing cluster control

function get_version {
  case $1 in
    ACCUMULO)
      (
        # run following in sub shell so it does not pollute
        . $UNO_HOME/conf/uno.conf
        echo $ACCUMULO_VERSION
      )
      ;;
    HADOOP)
      (
        # run following in sub shell so it does not pollute
        . $UNO_HOME/conf/uno.conf
        echo $HADOOP_VERSION
      )
      ;;
    ZOOKEEPER)
      (
        # run following in sub shell so it does not pollute
        . $UNO_HOME/conf/uno.conf
        echo $ZOOKEEPER_VERSION
      )
      ;;
    *)
      return 1
  esac
}

function start_cluster {
  $UNO setup accumulo
}

function setup_accumulo {
  $UNO setup accumulo --no-deps
}

function get_config_file {
  local ah=$(get_ah)
  cp "$ah/conf/$1" "$2"
}

function put_config_file {
  local ah=$(get_ah)
  cp "$1" "$ah/conf"
}

function put_server_code {
  local ah=$(get_ah)
  cp "$1" "$ah/lib/ext"
}

function start_accumulo {
  $UNO stop accumulo --no-deps &> /dev/null
  $UNO start accumulo --no-deps &> /dev/null
}

function stop_cluster {
  $UNO kill
}

#!/bin/bash

# setup correctly /opt/rasdaman/etc/rasmgr.conf
# using RASMGR_HOST_IP environment variable if set
if [ -z $RASMGR_HOST_IP ]; then
  export RASMGR_HOST_IP="localhost" 
fi

# Start rasdaman
$RMANBIN/start_rasdaman.sh --allow-root

# modifica Roberto Gter
#rm -f $RASMGR_CONF_FILE && sed "s/@hostname@/$RASMGR_HOST_IP/g" /rasmgr.conf.in > $RASMGR_CONF_FILE

if [ -z "$(ls -A $RMANDATA)" ]; then
	$RMANBIN/create_db.sh
fi


sh -c 'service postgresql start 2>&1'

sh -c 'sudo -u postgres createuser --no-password --createdb petauser 2>&1'
sh -c 'sudo -u postgres createdb --owner petauser petascopedb 2>&1'

## set tomcat9 permissions
#chown -R tomcat:tomcat /var/lib/tomcat9
#chown -R tomcat:tomcat /var/lib/tomcat9/webapps
#chown -R tomcat:tomcat /var/lib/tomcat9/webapps/* 
# Start tomcat
sh -c '/etc/init.d/tomcat9 start 2>&1'

# Start apache2
sh -c 'apache2ctl start 2>&1'


# Verify rasmgr is running
rasmgrnum=`ps aux | grep rasmgr | grep -v grep | wc -l`
rassrvnum=`ps aux | grep rasdaman | grep -v grep | wc -l`

terminate=0

#fwirasdatefile='lastfwi'
#fwirasdatepath='/opt/rasdaman/scripts'
#fwirasdate="$fwirasdatepath"/"$fwirasdatefile"
#today=`date "+%Y%m%d"`
##script per importare dati da Milanone (attualmente su Milanone)
#fwiimportscript='/opt/rasdaman/scripts/import_container.py'
#
##script per importare i risultati dell'interpolazione (container R4OI di M. Salvati)
#rasdaman_import='/opt/rasdaman/scripts/import_r4oi.py'
#exechour="06"
#
while [ true ]; do
#  #########################
#  #importazione  dati FWI
#  #########################
#  if [ ! -f $fwirasdate ]; then
#    /usr/bin/python $fwiimportscript
#    echo "$today" > $fwirasdate
#  else
#    hour=`date "+%H"`
#    
#    while IFS= read -r lastdate 
#    do
#      echo ""
#    done < $fwirasdate
#
#    if [ "$today" != "$lastdate" ]; then
#      if [ "$hour" == "$exechour" ]; then
#        /usr/bin/python $fwiimportscript
#        echo "$today" > $fwirasdate
#      fi
#    fi
#  fi
  #########################
  #importazione  dati OI
  #########################
#  S3CMD='s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt'
  # a causa di incongruenze nel passare le variabili d'ambiente questa riga qua definita è totalmente inutile
  # copio i file presenti in minio (solo ultima settimana)
  #$S3CMD ls s3://analisi/rh_ana* > elenco.txt
  #s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt ls s3://analisi/rh_ana* > elenco.txt
  #$S3CMD ls s3://analisi/rh_hdx* >> elenco.txt
  #s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt ls s3://analisi/rh_hdx* >> elenco.txt
  #$S3CMD ls s3://analisi/t2m_ana* >> elenco.txt
  #s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt ls s3://analisi/t2m_ana* >> elenco.txt
  #$S3CMD ls s3://analisi/t2m_bkg* >> elenco.txt
  #s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt ls s3://analisi/t2m_bkg* >> elenco.txt
  #$S3CMD ls s3://analisi/prec_ana* >> elenco.txt
  #s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt ls s3://analisi/prec_ana* >> elenco.txt
  
  # carico tutto da MINIO
#  s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt ls s3://analisi/* > elenco.txt
#  
#  #tail -n 8 elenco.txt > elenco1.txt
#  for i in $(cat elenco.txt |awk '{ print $4; }');
#     do
#      #$S3CMD --force get $i import/
#      s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$MINIO_HOST:$MINIO_PORT --host-bucket=$MINIO_HOST:$MINIO_PORT --config=config_minio.txt  --force get $i import/
#      # lancio lo script python per importare i dati su Rasdaman
#      # con il comando ${i:13} dovrei escludere la parte cpm s3://analisi dalla stringa $i
#      /usr/bin/python3 $rasdaman_import -f ${i:13} -p import
#      # dentro lo script python controllo l'esito e se positivo cancello il file da MINIO
#     done
#  
#  
  
  
	if [ $rasmgrnum = 0 ]; then
		echo "No rasdaman manager process alive. Terminate container."
		terminate=1
	else
		# Verify rasdaman worker processes
		if [ $rassrvnum = 0 ]; then
			echo "No rasdaman worker processes. Terminate container."
			terminate=1
		fi
	fi

	if [ $terminate != 0 ]; then
		break
	else
		sleep 300
	fi
done

# vim: set ts=2 number:


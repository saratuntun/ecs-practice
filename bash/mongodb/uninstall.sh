sudo service mongod stop
sudo apt-get purge mongodb-org*
sudo rm -r /var/log/mongodb /var/lib/mongodb
#
echo service mongod status

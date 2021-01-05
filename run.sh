#!/bin/zsh

# -----
# Set the project directory 
# 
PROJECT_DIR=/Users/troysellers/projects/appexchange-analytics-demo


# --- 
# The rest should remain as is
#

LOG_DIR=$PROJECT_DIR/logs
FILENAME=run.log

LOG_FILE=$LOG_DIR/$FILENAME

CLASSPATH=$PROJECT_DIR/.env:$PROJECT_DIR/target/analytics-jar-with-dependencies.jar

# mvn clean package > $LOG_DIR/build.log
START_TIME=$(date +%s)

echo "Starting Subscriber Snapshot ~~" > $LOG_FILE
java -cp $CLASSPATH com.grax.aus.SubscriberSnapshot >> $LOG_FILE
echo "\n---\n" >> $LOG_FILE

echo "Starting Package Usage Log ~~" >> $LOG_FILE
java -cp $CLASSPATH com.grax.aus.PackageUsageLog >> $LOG_FILE
echo "\n---\n" >> $LOG_FILE

echo "Starting Package Summary ~~" >> $LOG_FILE
java -cp $CLASSPATH com.grax.aus.PackageUsageSummary >> $LOG_FILE
echo "\n---\n" >> $LOG_FILE

END_TIME=$(date +%s)
echo "Process executed in $(($END_TIME - $START_TIME)) seconds" >> $LOG_FILE

mv $LOG_FILE $LOG_DIR/$(date +%s)$FILENAME




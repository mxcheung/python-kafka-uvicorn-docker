#!/bin/bash

# Specify the namespace and time range to search for completed jobs
NAMESPACE="my-namespace"
TIME_RANGE_MINUTES=60

# Calculate the time range for completed jobs
start_time=$(date -u -d "-$TIME_RANGE_MINUTES min" +"%Y-%m-%dT%H:%M:%SZ")
end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Monitor all recently completed jobs without overlap
last_time_checked=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
while true; do
    current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    if [ $(date -d "$current_time" +%s) -ge $(date -d "$last_time_checked +$TIME_RANGE_MINUTES min" +%s) ]; then
        completed_jobs=$(oc get jobs -n $NAMESPACE --field-selector=status.completionTime\>{$last_time_checked\}\,status.completionTime\<{$current_time} --output=jsonpath='{.items[*].metadata.name}')
        if [ -z "$completed_jobs" ]; then
            # No completed jobs, update last time checked and wait
            last_time_checked=$current_time
            sleep 5
            continue
        else
            # Print the results for each completed job
            for job in $completed_jobs; do
                job_status=$(oc get job $job -n $NAMESPACE --template='{{range .status.conditions}}{{if eq .type "Complete"}}{{.status}}{{end}}{{end}}')
                if [ "$job_status" == "True" ]; then
                    echo "Job '$job' succeeded with result: $(oc get job $job -n $NAMESPACE --template='{{range .status.conditions}}{{if eq .type "Complete"}}{{.message}}{{end}}{{end}}')"
                elif [ "$job_status" == "False" ]; then
                    echo "Job '$job' failed with reason: $(oc get job $job -n $NAMESPACE --template='{{range .status.conditions}}{{if eq .type "Failed"}}{{.reason}}: {{.message}}{{end}}{{end}}')"
                fi
            done
            last_time_checked=$current_time
        fi
    else
        # Time range not yet complete, wait and check again
        sleep 5
    fi
done

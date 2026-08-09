#!/bin/bash

set -euxo pipefail

# Make sure Pachyderm auth is enabled
command -v aws || pip install awscli --upgrade --user

function activate {
    pachctl config update context "$(pachctl config get active-context)" --pachd-address="$(minikube ip):30650"

    # Activate Pachyderm auth, if needed, and log in
    if ! pachctl auth list-admins; then
        admin="admin"
        echo "${admin}" | pachctl auth activate
    elif pachctl auth list-admins | grep "github:"; then
        admin="$( pachctl auth list-admins | grep 'github:' | head -n 1)"
        admin="${admin#github:}"
        echo "${admin}" | pachctl auth login
    else
        echo "Could not find a github user to log in as. Cannot get admin token"
        exit 1
    fi
}

function delete_all {
    if pachctl auth list-admins; then
        admin="$( pachctl auth list-admins | grep 'github:' | head -n 1)"
        admin="${admin#github:}"
        echo "${admin}" | pachctl auth login
    else
        echo "Could not find a github user to log in as. Cannot get admin token"
        exit 1
    fi
    echo "yes" | pachctl delete all
}

eval "set -- $( getopt -l "activate,delete-all" "--" "${0}" "${@}" )"
while true; do
    case "${1}" in
     --activate)
        activate
        shift
        ;;
     --delete-all)
        delete_all
        shift
        ;;
     --)
        shift
        break
        ;;
     *)
        echo "Unrecognized operation: ${1}"
        echo
        echo "Operation should be \"--activate\" or \"--delete-all\""
        shift
        ;;
    esac
done




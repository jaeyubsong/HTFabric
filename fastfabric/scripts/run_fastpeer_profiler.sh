#!/bin/bash
rm network_profile.log

function get_network {
  echo $(ifconfig ib0 | awk 'NR==7{print $5}')
}
network_before=$(get_network)
while true; do
  network_after=$(get_network)
  echo -e "$(date)\t$(($network_after - $network_before))" >> network_profile.log
  network_before=$network_after
  sleep 1
done

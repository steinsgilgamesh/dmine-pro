cnt=0
ptn3v_dir="/home/xuruiqi/Project/dmine-pro/build/ptn-3v/"
ptn4v_dir="/home/xuruiqi/Project/dmine-pro/build/ptn-4v/"
new_dir=""
for entry in $"/home/xuruiqi/Project/temporal_data/ICEWS2014_4_node_5_edge_rules_connected_202105251306"/*
do
  post_fix="${entry: -7}"
  if [[ ${post_fix} = rhs.txt ]]; then
    continue;
  fi
  the_other="${entry:$1:-7}"r"${entry: -6}"
  pre_fix="${entry:$1:-8}"
  query_name="${pre_fix##*/}"
  lhs_has4v=$(cat ${entry} | awk '{ if ($1 == 3 || $3 == 3) { print 1 } }' | wc -l)
  rhs_has4v=$(cat ${the_other} | awk '{ if ($1 == 3 || $3 == 3) { print 1 } }' | wc -l)
  # echo $has4v
  if [[ $lhs_has4v = "0" && $rhs_has4v = "0" ]]; then
    new_dir=${ptn3v_dir}
  else
    new_dir=${ptn4v_dir}
  fi
  new_lhs="${new_dir}$(basename ${entry})"
  echo "" > ${new_lhs}
  echo "edge_id:int,source_id:int,target_id:int,label_id:int,timestamp:int" >> ${new_lhs}
  tail -n +2 ${entry} | awk '{printf ""}'
  new_rhs="${new_dir}$(basename ${the_other})"
  echo "" > ${new_rhs}
  # post_fix="${entry: -7}"
  # # echo ${post_fix}
  # if [[ ${post_fix} = rhs.txt ]]; then
  # 	continue;
  # else
  # fi
  # pre_fix="${entry:$1:-8}"
  # query_name="${pre_fix##*/}"
  cnt=`expr $cnt + 1`
  if [[ "$cnt" = 100 ]]; then
  	break
  fi
done
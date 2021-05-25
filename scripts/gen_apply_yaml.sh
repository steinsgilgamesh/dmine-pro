is_lhs() {
    string="$1"
    substring="$2"
    if test "${string#*$substring}" != "$string"
    then
        return 0    # $substring is in $string
    else
        return 1    # $substring is not in $string
    fi
}

file=batch_apply.yaml
echo "" > $file

echo "Graph: 
  Name: icews14 
  Dir: /home/xuruiqi/Project/dmine-pro/build/icews14/ 
  VertexFile: icews2014.v.csv 
  EdgeFile: icews2014.e.csv

TGR:" >> $file

cnt=0
for entry in $"/home/xuruiqi/Project/dmine-pro/build/ptn-4v"/*
do
  ext="${entry: -3}"
  if [[ $ext != "txt" ]]; then
  	continue
  fi
  post_fix="${entry: -7}"
  # echo ${post_fix}
  if [[ ${post_fix} = rhs.txt ]]; then
  	continue;
  fi
  pre_fix="${entry:$1:-8}"
  query_name="${pre_fix##*/}"
  echo "${query_name}"
  echo "- Name: ${query_name}" >> $file
  echo "  Dir: $(dirname ${entry})/" >> $file
  echo "  VertexFile: node-4.v.csv" >> $file
  echo "  EdgeFile: ${query_name}_lhs.txt" >> $file
  echo "  LiteralX: empty.csv" >> $file
  echo "  LiteralY: ${query_name}_rhs.txt" >> $file
  cnt=`expr $cnt + 1`
  if [[ "$cnt" = 500 ]]; then
  	 break
  fi
done


echo "
OutputGraph:
  Name: tgr_matches
  Dir: /home/xuruiqi/Project/dmine-pro/build/matches/" >> $file
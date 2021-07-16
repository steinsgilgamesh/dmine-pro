#include "../tgraph_rule/tgraph.h"
#include "../tgraph_rule/comm.h"
#include "../tgraph_rule/config.h"
#include "../tgraph_rule/discover.h"
#include "../tgraph_rule/match.h"
#include "../tgraph_rule/partition.h"
#include "../tgraph_rule/tgraph_rule.h"

using namespace toy;

#define ENABLE_OPENMP

int worker_id_, worker_num_;
const int COORDINATOR = 0;
const int TAG_NUM = 2;
const int TAG_SUPP_Q = 1001;
const int TAG_SUPP_R = 1002;

/* Write match results into files */
void write_match(std::string output_path, const MatchResult &match_rlt) {
  if (match_rlt.empty()) return;
  std::ofstream outfile;
  outfile.open(output_path.data());
  auto &match = match_rlt[0];
  outfile << "match_id";
  for (auto iter : match) {
    outfile << "," << iter.first->id();
  }
  outfile << "\n";
  uint64_t cnt = 1;
  for (auto &match : match_rlt) {
    outfile << cnt++;
    for (auto iter : match) {
      outfile << "," << iter.second->id();
    }
    outfile << "\n";
  }
}


// void PrintTGR(const Q &query) {
//   std::cout << PLABEL << "TGR Detail  " << PLABEL << "\n";
//   std::cout << PLABEL << "V\n";
//   for (auto v_iter = query.VertexCBegin(); !v_iter.IsDone(); v_iter++) {
//     std::cout << PLABEL << v_iter->id() << "," << v_iter->label() << "\n";
//   }
//   std::cout << PLABEL << "E\n";
//   for (auto e_iter = query.EdgeCBegin(); !e_iter.IsDone(); e_iter++) {
//     auto attribute_ptr = e_iter->FindAttributePtr(TIME_KEY);
//     ATTRIBUTE_PTR_CHECK(attribute_ptr);
//     TIME_T timestamp = attribute_ptr->template value<int>();
//     std::cout << PLABEL << e_iter->id() << "," << e_iter->src_id() << ","
//               << e_iter->dst_id() << "," << e_iter->label() << "," << timestamp
//               << "\n";
//   }
// }



void write_matchX(
  TGraph &tg, Q &pattern, LinkBase &rule_rhs,
  std::string output_path, MatchResult &match_rlt,
  std::string pattern_name) {
  if (match_rlt.empty()) return;
  // preparation: get rhs id, vtx endpoints, query name, and timestamp offset
  if (rule_rhs.id_) { LOG_S("FATAL: LinkBase rhs with none-zero id!"); }

  // --------------- find rhs link edge ---------------
  auto e_ptr = pattern.FindEdge(rule_rhs.id_);
  if (e_ptr->id() != rule_rhs.id_) { LOG_S("FATAL: can not found eid-0 in query graph!"); }
  VID_T rhs_from = e_ptr->src_id();
  VID_T rhs_to = e_ptr->dst_id();
  auto rhs_label = e_ptr->label();
  auto attribute_ptr = e_ptr->FindAttributePtr(TIME_KEY);
  ATTRIBUTE_PTR_CHECK(attribute_ptr);
  TIME_T tstp_rhs = attribute_ptr->template value<int>();

  // --------------- find lhs link edge ---------------
  EID_T lhs_eid = 1;
  e_ptr = pattern.FindEdge(lhs_eid);
  if (e_ptr->id() != lhs_eid) { LOG_S("FATAL: can not found eid-1 in query graph!"); }
  VID_T lhs_from = e_ptr->src_id();
  VID_T lhs_to = e_ptr->dst_id();
  auto lhs_label = e_ptr->label();
  attribute_ptr = e_ptr->FindAttributePtr(TIME_KEY);
  ATTRIBUTE_PTR_CHECK(attribute_ptr);
  TIME_T tstp_lhs = attribute_ptr->template value<int>();

  TIME_T tstp_delta = tstp_rhs - tstp_lhs;
  LOG_S("delta time:", tstp_delta);

  // --------------- write_out ---------------
  std::ofstream outfile;
  outfile.open(output_path.data());
  auto &match = match_rlt[0];
  uint match_size = 0;
  for (auto iter : match) { match_size++; }
  std::vector<VID_T> vid_q2g(match_size);
  LOG_S("writing output with format of (match_id, src, elabel, dst, time) with pattern_name: ", pattern_name);
  // uint64_t cnt = 1;
  // auto link = tg_rule.GetLink();
  for (auto &match : match_rlt) {
    for (auto iter : match) {
      vid_q2g[iter.first->id()] = iter.second->id();
    }
    uint rep_cnt = 0;
    TGraph::const_iterator tg_iter = tg.CBegin();
    // compute time of lhs edge in data graph
    VID_T lhs_from_tg = vid_q2g[lhs_from];
    VID_T lhs_to_tg = vid_q2g[lhs_to];
    auto lhs_from_ptr = tg_iter->FindVertex(lhs_from_tg);
    auto lhs_to_ptr = tg_iter->FindVertex(lhs_to_tg);
    // vector<TIME_T> tstp_lhs_tg_bag;
    for (auto e_iter = lhs_from_ptr->OutEdgeBegin(lhs_label, lhs_to_ptr); !e_iter.IsDone(); e_iter++) {
      // if (multiple) { LOG_S("possiblly replications!"); }
      if (e_iter->label() != lhs_label) { LOG_S("WTF? label does NOT MATCH!"); }
      if (e_iter->src_id() != lhs_from_tg) { LOG_S("WTF? src does NOT MATCH!"); }
      if (e_iter->dst_id() != lhs_to_tg) { LOG_S("WTF? dst does NOT MATCH!"); }
      // if (cnt < 100) { LOG_S("got edge with label: ", e_iter->label()); }
      attribute_ptr = e_iter->FindAttributePtr(TIME_KEY);
      ATTRIBUTE_PTR_CHECK(attribute_ptr);
      TIME_T tstp_lhs_tg = attribute_ptr->template value<int>();
      // tstp_lhs_tg_bag.push_back(tstp_lhs_tg);
      // LOG_S("edge with tstp = ", tstp_lhs_tg);
      // outfile << pattern_name << "," << cnt << "," << vid_q2g[rhs_from] << "," << rhs_label
      //         << "," << vid_q2g[rhs_to] << "," << tstp_lhs_tg + tstp_delta << std::endl;
      // break;

      bool all_matched = true; // is true, if has corresponding tstp on all other edges
      for (EID_T p_eid = 2; p_eid < pattern.CountEdge(); ++p_eid) {
        auto e_ptr2 = pattern.FindEdge(p_eid);
        if (e_ptr2->id() != p_eid) { LOG_S("FATAL: can not found p_eid in query graph!"); }
        VID_T e_from = e_ptr2->src_id();
        VID_T e_to = e_ptr2->dst_id();
        auto e_label = e_ptr2->label();
        attribute_ptr = e_ptr2->FindAttributePtr(TIME_KEY);
        ATTRIBUTE_PTR_CHECK(attribute_ptr);
        TIME_T e_tstp = attribute_ptr->template value<int>();

        VID_T e_from_tg = vid_q2g[e_from];
        VID_T e_to_tg = vid_q2g[e_to];
        auto e_from_ptr = tg_iter->FindVertex(e_from_tg);
        auto e_to_ptr = tg_iter->FindVertex(e_to_tg);

        bool matched = false; // is true, if current edge satisfies tstp

        for (auto e_iter2 = e_from_ptr->OutEdgeBegin(e_label, e_to_ptr); !e_iter2.IsDone(); e_iter2++) {
          attribute_ptr = e_iter2->FindAttributePtr(TIME_KEY);
          ATTRIBUTE_PTR_CHECK(attribute_ptr);
          TIME_T e_tstp_tg = attribute_ptr->template value<int>();
          if ((e_tstp_tg - e_tstp) == (tstp_lhs_tg - tstp_lhs)) {
            matched = true;
            break;
          }
        }

        if (!matched) {
          all_matched = false;
          break;
        }
      }
      if (all_matched) {
        rep_cnt++;
        outfile << pattern_name << "," << vid_q2g[rhs_from] << "," << rhs_label
                << "," << vid_q2g[rhs_to] << "," << tstp_lhs_tg + tstp_delta << std::endl;
      }
    }
    if (rep_cnt == 0) {
      LOG_S("FATAL: Did not find ANY match!");
    } else if (rep_cnt > 1) {
      LOG_S("Found multiple match!");
    }

    // cnt++;
  }
}


void app_apply(Config &config, bool cal_percision = false) {
  // partition graph by time window
  if (worker_id_ == COORDINATOR) {
    std::vector<VID_T> send_flag;
    send_flag.emplace_back(1);
    Partition::DoPartition(config);
    for (int i = 0; i < COORDINATOR; i++) {
      SendVector(send_flag, i);
    }
    for (int i = COORDINATOR + 1; i < worker_num_; i++) {
      SendVector(send_flag, i);
    }
  } else {
    std::vector<VID_T> recv_flag(1);
    RecvVector(recv_flag, COORDINATOR);
  }

  // load temporal graph
  TGraph tg(config);
  tg.LoadTGraphByHash(worker_id_, worker_num_);
  // PrintTGR(tg.GraphStream()[0]);

  // load temporal graph rules
  TGraphRule tg_rule;
  tg_rule.LoadTGRs(config);
  const size_t tgr_size = tg_rule.Size();
  const auto &link = tg_rule.GetLink();
  // PrintLink(link);

  // init match method
  auto m_ptr = MatchFactory::GetMatchMethod(config);
  m_ptr->InitX(tg, link);

  LOG_S("Apply Start on worker ", worker_id_);
  auto t_begin = std::chrono::steady_clock::now();

  // assemble msg & result
  std::vector<std::vector<VID_T>> assemble_msg;
  assemble_msg.resize(worker_num_);
  std::vector<std::map<int, std::unordered_set<VID_T>>> assemble_rlt;
  assemble_rlt.resize(tgr_size);

  // message buffer
  // tgrSize, tagSize, tgrIndex1, tag1, size, x1,x2,..., tag2, size, x1,x2,...
  std::vector<VID_T> msg;
  if (worker_id_ != COORDINATOR) {
    msg.emplace_back(tgr_size);  // tgrSize
    if (cal_percision) {
      msg.emplace_back(TAG_NUM);  // tagSize
    } else {
      msg.emplace_back(1);
    }
  }

  // match
  auto &tg_rules = tg_rule.GetTGRSet();
  auto match_path = config.MatchFilePathVec();
  for (int i = 0; i < tgr_size; i++) {
    // PrintLink(link);
    LOG_S("Apply in TGR ", i + 1);
    auto &rule = tg_rules[i];
    if (worker_id_ != COORDINATOR) {
      msg.emplace_back(i);  // tgrIndex
    }
    // cal R block
    if (cal_percision) {
      MatchResult match_rlt;
      const auto &x_ptr = rule.FindConstVertex(link.from_);
      PrintTGR(rule);
#ifdef ENABLE_OPENMP
      m_ptr->DoMatchWithXParallel(tg, rule, match_rlt);
#else // ENABLE_OPENMP
      m_ptr->DoMatchWithX(tg, rule, match_rlt);
#endif // ENABLE_OPENMP
      // set msg
      if (worker_id_ == COORDINATOR) {
        std::unordered_set<VID_T> tmp_set;
        assemble_rlt[i][TAG_SUPP_R] = tmp_set;
        for (auto &match : match_rlt) {
          assemble_rlt[i][TAG_SUPP_R].emplace(match[x_ptr]->id());
        }
      } else {
        msg.emplace_back(TAG_SUPP_R);        // tag
        msg.emplace_back(match_rlt.size());  // size
        for (auto &match : match_rlt) {      // match x
          msg.emplace_back(match[x_ptr]->id());
        }
      }
    }
    // block end
    // cal Q block
    {
      Q query = rule;
      RemoveLink(query, link);
      MatchResult match_rlt;
      const auto &x_ptr = query.FindConstVertex(link.from_);
      LinkBase rule_rhs = link;
      // PrintTGR(query);
#ifdef ENABLE_OPENMP
      m_ptr->DoMatchWithXParallel(tg, query, match_rlt);
#else // ENABLE_OPENMP
      m_ptr->DoMatchWithX(tg, query, match_rlt);
#endif // ENABLE_OPENMP
      // set msg
      if (worker_id_ == COORDINATOR) {
        std::unordered_set<VID_T> tmp_set;
        assemble_rlt[i][TAG_SUPP_Q] = tmp_set;
        for (auto &match : match_rlt) {
          assemble_rlt[i][TAG_SUPP_Q].emplace(match[x_ptr]->id());
        }
        // write matches
        // write_match(match_path[i], match_rlt);
        write_matchX(tg, tg_rules[i], rule_rhs, match_path[i], match_rlt, config.GetPatternNameByOfst(i));
      } else {
        msg.emplace_back(TAG_SUPP_Q);        // tag
        msg.emplace_back(match_rlt.size());  // size
        for (auto &match : match_rlt) {      // match x
          msg.emplace_back(match[x_ptr]->id());
        }
      }
    }
    // block end
  }
  // send & recv msg
  if (worker_id_ == COORDINATOR) {
    for (int i = 0; i < COORDINATOR; i++) {
      RecvVector(assemble_msg[i], i);
    }
    for (int i = COORDINATOR + 1; i < worker_num_; i++) {
      RecvVector(assemble_msg[i], i);
    }
  } else {
    SendVector(msg, COORDINATOR);
  }
  // coordinator assemble
  if (worker_id_ == COORDINATOR) {
    for (int i = 0; i < worker_num_; i++) {
      if (i == COORDINATOR) continue;
      const auto &msg = assemble_msg[i];
      if (msg.empty()) continue;
      size_t pos = 0;
      const size_t tgr_size = msg.at(pos++);
      const size_t tag_size = msg.at(pos++);
      for (int tgr_i = 0; tgr_i < tgr_size; tgr_i++) {
        const size_t tgr_index = msg.at(pos++);
        auto &tag_map = assemble_rlt[tgr_index];
        for (int tag_i = 0; tag_i < tag_size; tag_i++) {
          auto tag = msg.at(pos++);
          auto &x_set = tag_map[tag];
          auto x_size = msg.at(pos++);
          for (int x_i = 0; x_i < x_size; x_i++) {
            x_set.emplace(msg.at(pos++));
          }
        }
      }
    }
    if (cal_percision) {
      std::vector<std::pair<GRADE_T, GRADE_T>> confs;
      confs.resize(tgr_size);
      for (int i = 0; i < tgr_size; i++) {
        auto &tag_map = assemble_rlt[i];
        confs[i].first = tag_map[TAG_SUPP_Q].size();
        confs[i].second = tag_map[TAG_SUPP_R].size();
      }
      // PrintConf(confs);
      config.PrintConfX(confs);
    } else {
      for (int i = 0; i < tgr_size; i++) {
        auto &tag_map = assemble_rlt[i];
        LOG("TGR Q Match Number: ", tag_map[TAG_SUPP_Q].size());
      }
    }
    auto t_end = std::chrono::steady_clock::now();
    LOG_S("Apply Time: ",
          std::chrono::duration<double>(t_end - t_begin).count());
  }
  LOG_S("Apply End on worker ", worker_id_);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    LOG_E("config yaml path must be given!");
    exit(-1);
  }

  InitMPIComm();
  GetMPIInfo(worker_id_, worker_num_);
  // init log level
  set_log_grade(LOG_T_GRADE);

  LOG_S("number of workers: ", worker_num_);

  // init config
  Config config;
  config.LoadApplyConfigByYaml(argv[1]);
  if (argc == 3) {
    config.LoadTimeWindowConfigByYaml(argv[2]);
  }
  config.PrintConfig();

  // run app
  app_apply(config, true);
  // app_apply(config);

  FinalizeMPIComm();
}

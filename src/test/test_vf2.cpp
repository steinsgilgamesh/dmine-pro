#include <ostream>

#include "gundam/algorithm/dp_iso.h"
#include "gundam/algorithm/vf2_boost.h"
#include "gundam/graph_type/large_graph.h"
#include "gundam/graph_type/large_graph2.h"
#include "gundam/io/csvgraph.h"
int main(int argc, char const *argv[]) {
//   /* code */
//   return 0;
// }
// int main() {
  using namespace GUNDAM;
  using GraphType = GUNDAM::LargeGraph<uint64_t, uint32_t, std::string,
        uint64_t, uint32_t, std::string>;

  using DataGraph = GUNDAM::LargeGraph<uint64_t, uint32_t, std::string,
        uint64_t, uint32_t, std::string>;
  using TargetVertexPtr = typename DataGraph::VertexConstPtr;
  DataGraph data_graph;
  using VertexConstPtr = typename DataGraph::VertexConstPtr;
  using VertexIDType = typename DataGraph::VertexType::IDType;
  using EdgeLabelType = typename DataGraph::EdgeType::LabelType;
  using EdgeConstPtr = typename GraphType::EdgeConstPtr;
  ReadCSVGraph(
    data_graph,
    argv[1],
    argv[2]);
  GraphType pattern;
  ReadCSVGraph(pattern,
               argv[3],
               argv[4]);
  auto t_begin = clock();
  int ans = GUNDAM::DPISO(pattern, data_graph, -1);
  auto t_end = clock();
  std::cout << "time = " << (1.0 * t_end - t_begin) / CLOCKS_PER_SEC
            << std::endl;
  std::cout << "size = " << ans << std::endl;
  return 0;
}
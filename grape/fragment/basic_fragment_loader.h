/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPE_FRAGMENT_BASIC_FRAGMENT_LOADER_H_
#define GRAPE_FRAGMENT_BASIC_FRAGMENT_LOADER_H_

#include "grape/communication/shuffle.h"
#include "grape/fragment/basic_fragment_loader_base.h"
#include "grape/fragment/rebalancer.h"
#include "grape/graph/edge.h"
#include "grape/graph/vertex.h"
#include "grape/vertex_map/vertex_map.h"
#include "mtkahip_interface.h"

namespace grape {

template <typename FRAG_T>
class BasicFragmentLoader : public BasicFragmentLoaderBase<FRAG_T> {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using internal_oid_t = typename InternalOID<oid_t>::type;
  using vid_t = typename fragment_t::vid_t;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;

 public:
  explicit BasicFragmentLoader(const CommSpec& comm_spec,
                               const LoadGraphSpec& spec)
      : BasicFragmentLoaderBase<FRAG_T>(comm_spec, spec) {
    if (spec_.idxer_type == IdxerType::kLocalIdxer) {
      LOG(ERROR) << "Global vertex map is required in BasicFragmentLoader";
      spec_.idxer_type = IdxerType::kHashMapIdxer;
    }
    if (spec_.rebalance) {
      LOG(ERROR) << "Rebalance is not supported in BasicFragmentLoader";
      spec_.rebalance = false;
    }
    recv_thread_running_ = false;
  }

  ~BasicFragmentLoader() {
    if (recv_thread_running_) {
      for (auto& ea : edges_to_frag_) {
        ea.Flush();
      }
      edge_recv_thread_.join();
    }
  }

  void AddVertex(const oid_t& id, const vdata_t& data) override {
    vertices_.emplace_back(id);
    vdata_.emplace_back(data);
  }

  void ConstructVertices() override {
    fid_t fid = comm_spec_.fid();
    fid_t fnum = comm_spec_.fnum();
    std::unique_ptr<IPartitioner<oid_t>> partitioner(nullptr);
    if (spec_.partitioner_type == PartitionerType::kHashPartitioner) {
      partitioner = std::unique_ptr<HashPartitioner<oid_t>>(
          new HashPartitioner<oid_t>(fnum));
    } else if (spec_.partitioner_type == PartitionerType::kMapPartitioner) {
      /* 
      std::vector<oid_t> all_vertices;
      sync_comm::FlatAllGather(vertices_, all_vertices, comm_spec_.comm());
      DistinctSort(all_vertices);

      partitioner = std::unique_ptr<MapPartitioner<oid_t>>(
          new MapPartitioner<oid_t>(fnum, all_vertices));
          */

      /*读划分文件
      std::vector<std::vector<oid_t>> oid_lists(fnum);
      std::string partition_file = "custom_partition.txt";
      VLOG(1) << "Worker " << comm_spec_.worker_id() << " loading custom partition from " << partition_file;
      std::ifstream fin(partition_file);
      if (!fin.is_open()) {
          LOG(FATAL) << "Worker " << comm_spec_.worker_id() 
               << " cannot open custom partition file: " << partition_file;
              }
          oid_t oid;
      fid_t target_fid;
      while (fin >> oid >> target_fid) {
        oid_lists[target_fid].push_back(oid);
      }
      fin.close();
      partitioner = std::unique_ptr<MapPartitioner<oid_t>>(
      new MapPartitioner<oid_t>(oid_lists));*/

      std::vector<std::vector<oid_t>> oid_lists(fnum);
      std::string csr_file = "csr.bin";
      VLOG(1) << "Worker " << comm_spec_.worker_id() 
              << " loading prebuilt CSR from " << csr_file;

      std::ifstream fin(csr_file, std::ios::binary);
      if (!fin) {
          LOG(FATAL) << "Worker " << comm_spec_.worker_id() 
                     << " cannot open prebuilt CSR file: " << csr_file;
      }

      int n, edge_count;
      fin.read(reinterpret_cast<char*>(&n), sizeof(int));
      fin.read(reinterpret_cast<char*>(&edge_count), sizeof(int));

      std::vector<int> xadj(n + 1);
      fin.read(reinterpret_cast<char*>(xadj.data()), (n + 1) * sizeof(int));

      std::vector<int> adjncy(edge_count);
      fin.read(reinterpret_cast<char*>(adjncy.data()), edge_count * sizeof(int));

      fin.close();

      // 分区结果数组（所有 worker 共享）
      std::vector<int> part(n);
int k=fnum;
      // 只在 coordinator (worker 0) 调用 mtkahip
      if (comm_spec_.worker_id() == 0) {
          int k = fnum;
          double imbalance = 1.03;
          bool suppress_output = true;
          int seed = 42;
          int mode = FASTSOCIALMULTITRY_PARALLEL;
          uint32_t num_threads = fnum;

          int edgecut = 0;

          VLOG(1) << "Coordinator (worker 0) calling mtkahip...";
          mtkahip(&n, nullptr, xadj.data(), nullptr, adjncy.data(),
                  &k, &imbalance, suppress_output, seed, mode, num_threads,
                  &edgecut, part.data());

          VLOG(1) << "Coordinator KaHIP partition done. Edge cut = " << edgecut;
      }

      // 广播 part 数组给所有 worker（确保所有 worker 使用相同的分区结果）
      sync_comm::Bcast(part, 0, comm_spec_.comm());

      VLOG(1) << "Worker " << comm_spec_.worker_id() 
              << " received broadcasted part array from coordinator";

      std::string v_file = "v.txt";
      VLOG(1) << "Worker " << comm_spec_.worker_id() 
              << " loading vertex ids from " << v_file;

      std::ifstream v_fin(v_file);
      if (!v_fin.is_open()) {
          LOG(FATAL) << "Worker " << comm_spec_.worker_id() 
                     << " cannot open vertex id file: " << v_file;
      }

      std::vector<oid_t> vertex_ids;
      vertex_ids.reserve(n);

      oid_t oid;
      while (v_fin >> oid) {
          vertex_ids.push_back(oid);
      }
      v_fin.close();

      if (vertex_ids.size() != static_cast<size_t>(n)) {
          LOG(FATAL) << "Vertex count mismatch: expected " << n 
                     << ", got " << vertex_ids.size();
      }

      // ======================================
      // 根据广播后的 part 分配到 oid_lists
      // ======================================
      for (int internal_id = 0; internal_id < n; ++internal_id) {
          fid_t target_fid = static_cast<fid_t>(part[internal_id]);
          if (target_fid >= fnum) {
              LOG(WARNING) << "Invalid fid " << target_fid 
                           << " for internal_id " << internal_id
                           << ", clipped to 0";
              target_fid = 0;
          }

          // 直接使用预存的原始 oid
          oid_lists[target_fid].push_back(vertex_ids[internal_id]);
      }

      // ======================================
      // 输出验证信息（只在主 worker 输出）
      // ======================================
      if (comm_spec_.worker_id() == 0) {
          std::cout << "\n==================================================" << std::endl;
          std::cout << "KaHIP 分区结果（共 " << fnum << " 个分区）" << std::endl;
          std::cout << "==================================================" << std::endl;

          // 按分区分组，存储每个分区的顶点
          std::vector<std::vector<oid_t>> partition_nodes(fnum);
          for (int internal_id = 0; internal_id < n; ++internal_id) {
              int p = part[internal_id];
              if (p >= 0 && p < k) {
                  partition_nodes[p].push_back(vertex_ids[internal_id]);
              }
          }

          // 输出每个分区的顶点
          for (int p = 0; p < k; ++p) {
              std::cout << "分区 " << p << "（共 " << partition_nodes[p].size() << " 个顶点）：";
              for (oid_t oid : partition_nodes[p]) {
                  std::cout << " " << oid;
              }
              std::cout << std::endl;
          }
          std::cout << "==================================================\n" << std::endl;
      }

      partitioner = std::unique_ptr<MapPartitioner<oid_t>>(
          new MapPartitioner<oid_t>(oid_lists));

    } else if (spec_.partitioner_type ==
               PartitionerType::kSegmentedPartitioner) {
      std::vector<oid_t> all_vertices;
      sync_comm::FlatAllGather(vertices_, all_vertices, comm_spec_.comm());
      DistinctSort(all_vertices);

      partitioner = std::unique_ptr<SegmentedPartitioner<oid_t>>(
          new SegmentedPartitioner<oid_t>(fnum, all_vertices));
    } else {
      LOG(FATAL) << "Unsupported partitioner type";
    }
    std::vector<std::vector<oid_t>> local_vertices_id;
    std::vector<std::vector<vdata_t>> local_vertices_data;
    this->ShuffleVertexData(vertices_, vdata_, local_vertices_id,
                            local_vertices_data, *partitioner);
    std::vector<oid_t> sorted_vertices;
    for (auto& buf : local_vertices_id) {
      sorted_vertices.insert(sorted_vertices.end(), buf.begin(), buf.end());
    }
    std::sort(sorted_vertices.begin(), sorted_vertices.end());

    VertexMapBuilder<oid_t, vid_t> builder(fid, fnum, std::move(partitioner),
                                           spec_.idxer_type);
    for (auto& v : sorted_vertices) {
      builder.add_vertex(v);
    }
    vertex_map_ =
        std::unique_ptr<VertexMap<oid_t, vid_t>>(new VertexMap<oid_t, vid_t>());
    builder.finish(comm_spec_, *vertex_map_);

    for (size_t buf_i = 0; buf_i < local_vertices_id.size(); ++buf_i) {
      std::vector<oid_t>& local_vertices = local_vertices_id[buf_i];
      std::vector<vdata_t>& local_vdata = local_vertices_data[buf_i];
      size_t local_vertices_num = local_vertices.size();
      for (size_t i = 0; i < local_vertices_num; ++i) {
        vid_t gid;
        if (vertex_map_->GetGid(local_vertices[i], gid)) {
          processed_vertices_.emplace_back(gid, std::move(local_vdata[i]));
        }
      }
    }

    edges_to_frag_.resize(fnum);
    for (fid_t fid = 0; fid < fnum; ++fid) {
      int worker_id = comm_spec_.FragToWorker(fid);
      edges_to_frag_[fid].Init(comm_spec_.comm(), edge_tag, 4096000);
      edges_to_frag_[fid].SetDestination(worker_id, fid);
      if (worker_id == comm_spec_.worker_id()) {
        edges_to_frag_[fid].DisableComm();
      }
    }
    edge_recv_thread_ =
        std::thread(&BasicFragmentLoader::edgeRecvRoutine, this);
    recv_thread_running_ = true;
  }

  void AddEdge(const oid_t& src, const oid_t& dst,
               const edata_t& data) override {
    vid_t src_gid, dst_gid;
    if (vertex_map_->GetGid(src, src_gid) &&
        vertex_map_->GetGid(dst, dst_gid)) {
      fid_t src_fid = id_parser_.get_fragment_id(src_gid);
      fid_t dst_fid = id_parser_.get_fragment_id(dst_gid);
      edges_to_frag_[src_fid].Emplace(src_gid, dst_gid, data);
      if (src_fid != dst_fid) {
        edges_to_frag_[dst_fid].Emplace(src_gid, dst_gid, data);
      }
    }
  }

  void ConstructFragment(std::shared_ptr<fragment_t>& fragment) override {
    for (auto& ea : edges_to_frag_) {
      ea.Flush();
    }

    edge_recv_thread_.join();
    recv_thread_running_ = false;

    MPI_Barrier(comm_spec_.comm());

    got_edges_.emplace_back(
        std::move(edges_to_frag_[comm_spec_.fid()].buffers()));
    edges_to_frag_[comm_spec_.fid()].Clear();

    std::vector<Edge<vid_t, edata_t>> processed_edges;
    for (auto& buffers : got_edges_) {
      foreach_rval(buffers, [&processed_edges](vid_t&& src, vid_t&& dst,
                                               edata_t&& data) {
        processed_edges.emplace_back(src, dst, std::move(data));
      });
    }

    fragment = std::make_shared<fragment_t>();
    fragment->Init(comm_spec_, spec_.directed, std::move(vertex_map_),
                   processed_vertices_, processed_edges);

    this->InitOuterVertexData(fragment);
  }

 private:
  void edgeRecvRoutine() {
    ShuffleIn<vid_t, vid_t, edata_t> data_in;
    data_in.Init(comm_spec_.fnum(), comm_spec_.comm(), edge_tag);
    fid_t dst_fid;
    int src_worker_id;
    while (!data_in.Finished()) {
      src_worker_id = data_in.Recv(dst_fid);
      if (src_worker_id == -1) {
        break;
      }
      if (dst_fid == comm_spec_.fid()) {
        got_edges_.emplace_back(std::move(data_in.buffers()));
        data_in.Clear();
      }
    }
  }

  std::vector<oid_t> vertices_;
  std::vector<vdata_t> vdata_;

  std::vector<internal::Vertex<vid_t, vdata_t>> processed_vertices_;

  std::unique_ptr<VertexMap<oid_t, vid_t>> vertex_map_;

  std::vector<ShuffleOut<vid_t, vid_t, edata_t>> edges_to_frag_;
  std::thread edge_recv_thread_;
  bool recv_thread_running_;

  std::vector<ShuffleBufferTuple<vid_t, vid_t, edata_t>> got_edges_;

  std::vector<vid_t> src_gid_list_;
  std::vector<vid_t> dst_gid_list_;
  std::vector<edata_t> edata_;

  using BasicFragmentLoaderBase<FRAG_T>::comm_spec_;
  using BasicFragmentLoaderBase<FRAG_T>::spec_;
  using BasicFragmentLoaderBase<FRAG_T>::id_parser_;

  using BasicFragmentLoaderBase<FRAG_T>::edge_tag;
};

};  // namespace grape

#endif  // GRAPE_FRAGMENT_BASIC_FRAGMENT_LOADER_H_

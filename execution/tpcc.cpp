#include <string>

#include "execution/execution.h"
#include "execution/tpcc/transaction.h"

namespace slog {

using std::stoi;
using std::stoll;

TPCCExecution::TPCCExecution(const SharderPtr& sharder, const std::shared_ptr<Storage>& storage)
    : sharder_(sharder), storage_(storage) {}

void TPCCExecution::Execute(Transaction& txn) {
  auto txn_adapter = std::make_shared<tpcc::TxnStorageAdapter>(txn);

  if (txn.code().procedures().empty() || txn.code().procedures(0).args().empty()) {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Invalid code");
    return;
  }

  std::ostringstream abort_reason;
  const auto& args = txn.code().procedures(0).args();
  const auto& txn_name = args[0];
  bool result = false;
  if (txn_name == "new_order") {
    if (args.size() != 7 || txn.code().procedures_size() != 11) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("new_order: Invalid number of arguments");
      return;
    }
    int w_id = stoi(args[1]);
    int d_id = stoi(args[2]);
    int c_id = stoi(args[3]);
    int o_id = stoi(args[4]);
    int64_t datetime = stoll(args[5]);
    int w_i_id = stoll(args[6]);
    std::array<tpcc::NewOrderTxn::OrderLine, 10> ol;
    for (int i = 0; i < static_cast<int>(ol.size()); i++) {
      const auto& order_line = txn.code().procedures(i + 1);
      if (order_line.args_size() != 4) {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("new_order: Invalid number of arguments for order line");
        return;
      }
      int ol_id = stoi(order_line.args(0));
      int supply_w_id = stoi(order_line.args(1));
      int item_id = stoi(order_line.args(2));
      int quantity = stoi(order_line.args(3));
      ol[i] = tpcc::NewOrderTxn::OrderLine{.id = ol_id, .supply_w_id = supply_w_id, .item_id = item_id, .quantity = quantity};
    }

    tpcc::NewOrderTxn new_order(txn_adapter, w_id, d_id, c_id, o_id, datetime, w_i_id, ol);
    result = new_order.Execute();

  } else if (txn_name == "payment") {
    if (args.size() != 9) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("payment: Invalid number of arguments");
      return;
    }
    int w_id = stoi(args[1]);
    int d_id = stoi(args[2]);
    int c_w_id = stoi(args[3]);
    int c_d_id = stoi(args[4]);
    int c_id = stoi(args[5]);
    int64_t amount = stoll(args[6]);
    int64_t datetime = stoll(args[7]);
    int h_id = stoi(args[8]);

    tpcc::PaymentTxn payment(txn_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
    result = payment.Execute();

  } else if (txn_name == "order_status") {
    if (args.size() != 5) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("order_status: Invalid number of arguments");
      return;
    }
    int w_id = stoi(args[1]);
    int d_id = stoi(args[2]);
    int c_id = stoi(args[3]);
    int o_id = stoi(args[4]);
  
    tpcc::OrderStatusTxn order_status(txn_adapter, w_id, d_id, c_id, o_id);
    result = order_status.Execute();
  
  } else if (txn_name == "deliver") {
    if (args.size() != 7) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("deliver: Invalid number of arguments");
      return;
    }
    int w_id = stoi(args[1]);
    int d_id = stoi(args[2]);
    int no_o_id = stoi(args[3]);
    int c_id = stoi(args[4]);
    int o_carrier = stoi(args[5]);
    int64_t datetime = stoll(args[6]);
    
    tpcc::DeliverTxn deliver(txn_adapter, w_id, d_id, no_o_id, c_id, o_carrier, datetime);
    result = deliver.Execute();

  } else if (txn_name == "stock_level") {
    if (args.size() != 4 || txn.code().procedures_size() != 2) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("stock_level: Invalid number of arguments");
      return;
    }
    int w_id = stoi(args[1]);
    int d_id = stoi(args[2]);
    int o_id = stoi(args[3]);
    std::array<int, tpcc::StockLevelTxn::kTotalItems> i_ids;
    const auto& item_ids = txn.code().procedures(1);
    if ( txn.code().procedures(1).args_size() != tpcc::StockLevelTxn::kTotalItems) {
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_reason("stock_level: Invalid number of items");
      return;
    }
    for (int i = 0; i < tpcc::StockLevelTxn::kTotalItems; i++) {
      i_ids[i] = stoi(item_ids.args(i));
    }

    tpcc::StockLevelTxn stock_level(txn_adapter, w_id, d_id, o_id, i_ids);
    result = stock_level.Execute();

  } else {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Unknown procedure name");
    return;
  }
  if (result) {
    txn.set_status(TransactionStatus::COMMITTED);
    ApplyWrites(txn, sharder_, storage_);
  } {
    txn.set_status(TransactionStatus::ABORTED);
    txn.set_abort_reason("Aborted by a TPC-C txn");
  }
}

}  // namespace slog
#include "common/service_utils.h"
#include "module/client.h"

DEFINE_string(host, "localhost", "Hostname of the SLOG server to connect to");
DEFINE_uint32(port, 5051, "Port number of the SLOG server to connect to");

using namespace slog;

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);

  auto context = std::make_shared<zmq::context_t>(1);
  Client client(context, FLAGS_host, FLAGS_port);
  client.Start();
 
  return 0;
}
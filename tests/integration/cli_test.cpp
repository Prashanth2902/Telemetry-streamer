// End-to-end tests of the tstream executable (SPEC §13 "Integration").
// Each test spawns the real binary, so sanitizer presets check the whole
// program, including shutdown.

#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace {

using namespace std::chrono_literals;
namespace fs = std::filesystem;
using ::testing::HasSubstr;

const fs::path kBinary = TSTREAM_BINARY;
const fs::path kDefaultConfig = fs::path(TSTREAM_SOURCE_DIR) / "configs" / "default.json";

struct RunResult {
  int exit_code = -1;  // -1 if killed by a signal
  std::string out;
  std::string err;
};

std::string read_file(const fs::path& path) {
  std::ifstream in(path);
  std::ostringstream text;
  text << in.rdbuf();
  return text.str();
}

class CliTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir_ = fs::temp_directory_path() /
           ("tstream_cli_" + std::string(info->name()) + "_" + std::to_string(::getpid()));
    fs::create_directories(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }

  fs::path write_config(const nlohmann::json& config) {
    const fs::path path = dir_ / "config.json";
    std::ofstream(path) << config.dump(2);
    return path;
  }

  static nlohmann::json default_config() {
    return nlohmann::json::parse(read_file(kDefaultConfig));
  }

  // Runs tstream with `args`. If `signal_after` is set, sends `signal` after
  // that delay. Fails the test (and kills the process) if it doesn't exit
  // within 60 s.
  RunResult run(const std::vector<std::string>& args,
                std::optional<std::chrono::milliseconds> signal_after = std::nullopt,
                int signal = SIGINT) {
    const fs::path out_path = dir_ / "stdout.txt";
    const fs::path err_path = dir_ / "stderr.txt";

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, out_path.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, err_path.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC, 0644);

    std::vector<std::string> storage{kBinary.string()};
    storage.insert(storage.end(), args.begin(), args.end());
    std::vector<char*> argv;
    argv.reserve(storage.size() + 1);
    for (auto& s : storage) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);

    pid_t pid = 0;
    const int rc = posix_spawn(&pid, kBinary.c_str(), &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    EXPECT_EQ(rc, 0) << "posix_spawn failed for " << kBinary;
    if (rc != 0) {
      return {};
    }

    if (signal_after) {
      std::this_thread::sleep_for(*signal_after);
      ::kill(pid, signal);
    }

    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + 60s;
    while (::waitpid(pid, &status, WNOHANG) == 0) {
      if (std::chrono::steady_clock::now() > deadline) {
        ::kill(pid, SIGKILL);
        ::waitpid(pid, &status, 0);
        ADD_FAILURE() << "tstream did not exit within 60 s";
        break;
      }
      std::this_thread::sleep_for(10ms);
    }

    RunResult result;
    result.exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    result.out = read_file(out_path);
    result.err = read_file(err_path);
    return result;
  }

  fs::path dir_;
};

struct Totals {
  bool found = false;  // false if stdout had no summary line
  std::uint64_t generated = 0;
  std::uint64_t dropped = 0;
  std::uint64_t delivered = 0;
  std::uint64_t lost = 0;
  std::uint64_t batches = 0;
};

Totals parse_totals(const std::string& out) {
  static const std::regex kTotal(
      R"(summary total generated=(\d+) dropped=(\d+) delivered=(\d+) lost=(\d+) batches=(\d+))");
  std::smatch m;
  if (!std::regex_search(out, m, kTotal)) {
    return {};
  }
  return {.found = true,
          .generated = std::stoull(m[1]),
          .dropped = std::stoull(m[2]),
          .delivered = std::stoull(m[3]),
          .lost = std::stoull(m[4]),
          .batches = std::stoull(m[5])};
}

// Sum of "batch N: M samples" lines, and how many there were.
std::pair<std::uint64_t, std::uint64_t> sum_batch_lines(const std::string& out) {
  static const std::regex kBatch(R"(^batch \d+: (\d+) samples)");
  std::uint64_t samples = 0;
  std::uint64_t lines = 0;
  std::istringstream in(out);
  std::string line;
  while (std::getline(in, line)) {
    std::smatch m;
    if (std::regex_search(line, m, kBatch)) {
      samples += std::stoull(m[1]);
      ++lines;
    }
  }
  return {samples, lines};
}

void expect_clean_shutdown(const RunResult& r) {
  EXPECT_EQ(r.exit_code, 0) << r.err;
  const auto totals = parse_totals(r.out);
  ASSERT_TRUE(totals.found) << "no summary line in stdout:\n" << r.out;
  EXPECT_EQ(totals.lost, 0U);
  EXPECT_EQ(totals.generated, totals.dropped + totals.delivered);
  // Every delivered sample appears in a printed batch, and vice versa.
  const auto [batch_samples, batch_lines] = sum_batch_lines(r.out);
  EXPECT_EQ(batch_samples, totals.delivered);
  EXPECT_EQ(batch_lines, totals.batches);
  EXPECT_THAT(r.err, HasSubstr("stopped cleanly"));
}

TEST_F(CliTest, DefaultConfigRunsForDurationAndPrintsBatches) {
  const auto r = run({"--config", kDefaultConfig.string(), "--duration", "1"});
  expect_clean_shutdown(r);
  EXPECT_THAT(r.out, HasSubstr("batch 0: "));
  const auto totals = parse_totals(r.out);
  ASSERT_TRUE(totals.found);
  EXPECT_EQ(totals.dropped, 0U);      // default config is sized for zero drops
  EXPECT_GT(totals.delivered, 500U);  // ~1000 from the 1 kHz IMU; slack for sanitizers
}

TEST_F(CliTest, SigintTriggersGracefulShutdown) {
  const auto r = run({"--config", kDefaultConfig.string()}, 700ms, SIGINT);
  expect_clean_shutdown(r);
}

TEST_F(CliTest, SigtermTriggersGracefulShutdown) {
  const auto r = run({"--config", kDefaultConfig.string()}, 700ms, SIGTERM);
  expect_clean_shutdown(r);
}

// With a 10 s max delay and a huge batch, nothing flushes on its own: every
// sample has to come out of the final flush at shutdown.
TEST_F(CliTest, FinalBatchIsFlushedOnShutdown) {
  auto config = default_config();
  config["batch"]["max_samples"] = 65536;
  config["batch"]["max_delay_us"] = 10'000'000;
  const auto r = run({"--config", write_config(config).string(), "--duration", "0.5"});
  expect_clean_shutdown(r);
  const auto totals = parse_totals(r.out);
  ASSERT_TRUE(totals.found);
  EXPECT_EQ(totals.batches, 1U);
  EXPECT_GT(totals.delivered, 0U);
}

TEST_F(CliTest, InvalidConfigExitsWithCode2) {
  auto config = default_config();
  config["sources"][0]["ring_capacity"] = 1000;
  const auto r = run({"--config", write_config(config).string(), "--duration", "1"});
  EXPECT_EQ(r.exit_code, 2);
  EXPECT_THAT(r.err, HasSubstr("power of two"));
  EXPECT_THAT(r.out, ::testing::Not(HasSubstr("batch")));
}

TEST_F(CliTest, MissingConfigFileExitsWithCode2) {
  const auto r = run({"--config", (dir_ / "missing.json").string(), "--duration", "1"});
  EXPECT_EQ(r.exit_code, 2);
  EXPECT_THAT(r.err, HasSubstr("cannot open config file"));
}

TEST_F(CliTest, BadArgumentsPrintUsage) {
  for (const auto& args :
       std::vector<std::vector<std::string>>{{},
                                             {"--config"},
                                             {"--duration", "1"},
                                             {"--config", "x", "--duration", "-1"},
                                             {"--bogus"}}) {
    const auto r = run(args);
    EXPECT_EQ(r.exit_code, 1);
    EXPECT_THAT(r.err, HasSubstr("usage: tstream"));
  }
}

}  // namespace

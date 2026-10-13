// Helpers for integration tests that run the real executables.

#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace tstream::testing {

// Base fixture: a fresh temporary directory per test, in dir_, removed after.
class TempDirTest : public ::testing::Test {
 protected:
  // Creates /tmp/tstream_<suite>_<test>_<pid>.
  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    dir_ = std::filesystem::temp_directory_path() /
           ("tstream_" + std::string(info->test_suite_name()) + "_" + std::string(info->name()) +
            "_" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir_);
  }
  // Removes the directory and everything in it.
  void TearDown() override { std::filesystem::remove_all(dir_); }

  std::filesystem::path dir_;
};

// Whole file as a string; empty if it doesn't exist.
inline std::string read_file(const std::filesystem::path& path) {
  std::ifstream in(path);
  std::ostringstream text;
  text << in.rdbuf();
  return text.str();
}

// A child process with stdout and stderr redirected to files.
class ChildProcess {
 public:
  // Starts `binary args...`. Fails the test if posix_spawn fails.
  ChildProcess(const std::filesystem::path& binary, const std::vector<std::string>& args,
               const std::filesystem::path& out, const std::filesystem::path& err) {
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, out.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, err.c_str(),
                                     O_WRONLY | O_CREAT | O_TRUNC, 0644);
    std::vector<std::string> storage{binary.string()};
    storage.insert(storage.end(), args.begin(), args.end());
    std::vector<char*> argv;
    argv.reserve(storage.size() + 1);
    for (auto& s : storage) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);
    const int rc = posix_spawn(&pid_, binary.c_str(), &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    EXPECT_EQ(rc, 0) << "posix_spawn failed for " << binary;
    if (rc != 0) {
      pid_ = -1;
    }
  }
  // Owns a pid: not copyable; moving is not needed.
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;
  ChildProcess(ChildProcess&&) = delete;
  ChildProcess& operator=(ChildProcess&&) = delete;
  // Kills and reaps the child if a test left it running.
  ~ChildProcess() {
    if (pid_ > 0 && !exit_code_) {
      ::kill(pid_, SIGKILL);
      int status = 0;
      ::waitpid(pid_, &status, 0);
    }
  }

  // Sends `sig` to the child.
  void signal(int sig) const {
    if (pid_ > 0) {
      ::kill(pid_, sig);
    }
  }

  // Waits for exit and returns the exit code (-1 if killed by a signal).
  // After `timeout` the child is killed and the test fails.
  int wait(std::chrono::milliseconds timeout = std::chrono::seconds(60)) {
    if (exit_code_) {
      return *exit_code_;
    }
    if (pid_ <= 0) {
      return -1;
    }
    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (::waitpid(pid_, &status, WNOHANG) == 0) {
      if (std::chrono::steady_clock::now() > deadline) {
        ::kill(pid_, SIGKILL);
        ::waitpid(pid_, &status, 0);
        ADD_FAILURE() << "child " << pid_ << " did not exit within " << timeout.count() << " ms";
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    exit_code_ = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    return *exit_code_;
  }

 private:
  pid_t pid_ = -1;
  std::optional<int> exit_code_;
};

// The first line of `text` starting with `prefix`, or "" if none.
inline std::string find_line(const std::string& text, const std::string& prefix) {
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    if (line.starts_with(prefix)) {
      return line;
    }
  }
  return "";
}

// The "key=value" tokens of a line, as a map.
inline std::map<std::string, std::string> key_values(const std::string& line) {
  std::map<std::string, std::string> out;
  std::istringstream in(line);
  std::string token;
  while (in >> token) {
    const auto eq = token.find('=');
    if (eq != std::string::npos && eq > 0) {
      out[token.substr(0, eq)] = token.substr(eq + 1);
    }
  }
  return out;
}

// key_values(...)[key] as an integer; fails the test if missing.
inline std::uint64_t field(const std::map<std::string, std::string>& fields,
                           const std::string& key) {
  const auto it = fields.find(key);
  EXPECT_NE(it, fields.end()) << "missing field " << key;
  return it == fields.end() ? 0 : std::stoull(it->second);
}

}  // namespace tstream::testing

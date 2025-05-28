# Spe

> Scalable Process Executor (Spe) — a project for the Programming Scalable Systems course at UPM (Universidad Politécnica de Madrid).

This repository contains the source code and setup instructions for the final project of the Programming Scalable Systems course. For more information about the assignment, refer to the official [course PDF on Moodle](https://moodle.upm.es/titulaciones/oficiales/pluginfile.php/11992901/mod_resource/content/6/jobs_and_tasks.pdf).

---

## 🧠 Overview

**Spe (Scalable Process Executor)** is a distributed job processing engine implemented in Elixir. It allows concurrent execution of jobs composed of tasks, each with dependencies and timeout constraints. Tasks are scheduled and executed respecting their dependencies using concepts from OTP such as GenServers, Supervisors, and the Phoenix PubSub library. The goal is to ensure reliability, fault tolerance, and concurrency while processing tasks within jobs.

A job is described as a DAG (Directed Acyclic Graph) of tasks where each task can depend on others. The engine supports job submission, controlled concurrency through a configurable worker limit, failure recovery, and result broadcasting via Phoenix PubSub.

---

## 📦 Project Structure

This project follows a standard Mix structure and includes source code, tests, and presentation slides.

```
.
├── AUTHORS                    # List of project contributors
├── README.md                 # Project documentation
├── lib                       # Main application source files
│ ├── spe                  # Internal modules
│ │ ├── application.ex   # Spe application supervision tree
│ │ ├── job_manager.ex   # Manages job assignments and coordination
│ │ └── task_worker.ex    # Executes individual tasks
│ └── spe.ex               # Entry point module
├── mix.exs                   # Mix project configuration
├── mix.lock                  # Locked dependency versions
├── slides                    # LaTeX source for presentation
│ ├── main.tex             # Main presentation file
│ └── style/style.tex      # Custom Beamer theme or styling
├── slides 2                  # Possibly older or alternative slides (consider renaming)
└── test                      # Test suite
    ├── job_manager_test.exs # Unit tests for JobManager
    ├── spe_test.exs         # General application tests
    └── test_helper.exs      # Test environment setup
```

---

## 🚀 Getting Started

### 🔐 Access

If you're reading this, you have access to the private GitHub repository.

### 📅 Clone the Repository

```bash
git clone git@github.com:helarteDeProgramar/spe.git
cd spe
```

### 💠 Install Dependencies

```bash
mix deps.get
```

### ▶️ Run the Project

To check that everything is set up correctly:

```bash
mix run
```

---

## 🧪 Running Tests

Run the test suite with:

```bash
mix test
```

---

## 📦 Installation (If Published on Hex)

If this package is published on [Hex.pm](https://hex.pm), you can include it in your own Elixir project:

```elixir
def deps do
  [
    {:spe, "~> 0.1.0"}
  ]
end
```

---

## 📚 Documentation

Documentation can be generated locally using [ExDoc](https://github.com/elixir-lang/ex_doc):

```bash
mix docs
```

Once published, documentation will be available at [HexDocs](https://hexdocs.pm/spe).

---

## 👥 Contributors

* [Germán Ruiz Cabello](https://github.com/geermanruuiz) — Main developer
* [Alejandro Soriano Compta](https://github.com/Alexsq1) — Main developer
* [Rubén García-Patos Benito](https://github.com/helarteDeProgrmar) — Main developer


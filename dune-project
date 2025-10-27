(lang dune 3.20)

(name ocaml-slurm)

(generate_opam_files true)

(source
 (github mtelvers/ocaml-slurm))

(authors "Mark Elvers <mark.elvers@tunbury.org>")

(maintainers "Mark Elvers <mark.elvers@tunbury.org>")

(license MIT)

(documentation https://github.com/ocaml/ocaml-slurm)

(package
 (name ocaml-slurm)
 (synopsis "OCaml client library for Slurm REST API")
 (description "A client library for interacting with the Slurm workload manager via its REST API (slurmrestd). Supports job submission, status querying, cancellation, and JWT authentication.")
 (depends
  (ocaml (>= 4.14))
  dune
  lwt
  cohttp-lwt-unix
  yojson
  (ppx_deriving_yojson (>= 3.7.0))
  logs
  fmt)
 (tags
  ("slurm" "hpc" "rest" "api" "workload-manager")))

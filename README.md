# ocaml-slurm

OCaml client library for the [Slurm](https://slurm.schedmd.com/) workload manager REST API.

## Features

- Job submission via the Slurm REST API (slurmrestd)
- Job status querying
- Job cancellation
- JWT authentication with automatic token generation via `scontrol`

## Requirements

- OCaml >= 4.14
- Slurm with `slurmrestd` daemon running
- Access to `scontrol` command for JWT token generation

## Installation

```bash
opam pin add ocaml-slurm https://github.com/mtelvers/ocaml-slurm.git
opam install ocaml-slurm
```

## Example Program

The package includes an example program `bin/main.ml` that demonstrates the library's capabilities:

```bash
dune exec slurm-example
```

This will:
1. Generate a JWT token
2. Submit a test job with an invalid feature (demonstrating error handling)
3. Submit a valid test job
4. Poll for job status until completion

## API Reference

### Types

- `Slurm_rest.auth` - Authentication configuration (JWT or Unix socket)
- `Slurm_rest.config` - Client configuration
- `Slurm_rest.job_state` - Job state variant (Pending, Running, Completed, etc.)
- `Slurm_rest.job_info` - Job information returned by Slurm
- `Slurm_rest.job_submit` - Job submission specification

### Functions

- `generate_token ~username ?lifespan ()` - Generate JWT token using scontrol
- `make_config ~base_url ~auth ?api_version ()` - Create client configuration
- `submit_job config spec` - Submit a job to Slurm
- `get_job config job_id` - Get information about a specific job
- `get_jobs config ?job_ids ()` - Get information about multiple jobs
- `cancel_job config job_id` - Cancel a running job
- `job_state_of_string state` - Convert string to job_state variant

## Slurm REST API Setup

The library requires `slurmrestd` to be running. See the [Slurm REST API documentation](https://slurm.schedmd.com/rest.html) for setup instructions. See [mtelvers/slurm-ansible](https://github.com/mtelvers/slurm-ansible)

Typically:
1. Configure `AuthAltTypes=auth/jwt` in `slurm.conf` and `slurmdbd.conf`
2. Generate JWT key at `/var/spool/slurmctld/jwt_hs256.key`
3. Start slurmrestd: `slurmrestd -s openapi/v0.0.40 0.0.0.0:6820`

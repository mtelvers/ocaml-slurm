open Lwt.Syntax
open Slurm

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Debug)

(* Create a job spec with common defaults *)
let make_job_spec ~name ~script ?constraints () : Slurm_rest.job_submit =
  {
    name;
    script;
    account = Some "ocaml";
    partition = None;
    nodes = Some "1";
    tasks = Some 1;
    cpus_per_task = None;
    memory_mb = None;
    time_limit = None;
    environment = [ ("PATH", "/usr/bin:/bin") ];
    constraints;
    working_directory = Some "/tmp";
    stdout_path = Some "/tmp/slurm-test-%j.out";
    stderr_path = None;
  }

(* Poll job status until terminal state, calling handler when done *)
let poll_job_until_done config job_id ~on_terminal_state =
  let rec poll count =
    if count > 20 then
      let* () = Lwt_io.printf "\nGiving up after 20 polls\n" in
      Lwt.return_unit
    else
      let* () = Lwt_io.printf "Polling job status (attempt %d)...\n" count in
      let* get_result = Slurm_rest.get_job config job_id in
      match get_result with
      | Error (`Msg msg) ->
          let* () = Lwt_io.eprintf "Failed to get job status: %s\n" msg in
          exit 1
      | Ok job_info -> (
          let state = Slurm_rest.job_state_of_string job_info.job_state in
          let* () = Lwt_io.printf "  State: %s\n" job_info.job_state in
          let* () =
            match job_info.exit_code with
            | Some code -> Lwt_io.printf "  Exit code: %d\n" code
            | None -> Lwt_io.printf "  Exit code: (not set)\n"
          in
          let* () =
            match job_info.signal with
            | Some sig_ -> Lwt_io.printf "  Signal: %d\n" sig_
            | None -> Lwt_io.printf "  Signal: (not set)\n"
          in
          match state with
          | Slurm_rest.Pending
          | Slurm_rest.Running
          | _
            when state <> Slurm_rest.Completed && state <> Slurm_rest.Failed && state <> Slurm_rest.Cancelled && state <> Slurm_rest.Timeout ->
              let* () = Lwt_io.printf "  (waiting...)\n\n" in
              let* () = Lwt_unix.sleep 2.0 in
              poll (count + 1)
          | terminal_state -> on_terminal_state terminal_state job_info)
  in
  poll 1

(* Submit job and poll until completion *)
let submit_and_poll config job_spec ~on_terminal_state =
  let* submit_result = Slurm_rest.submit_job config job_spec in
  match submit_result with
  | Error (`Msg msg) ->
      let* () = Lwt_io.eprintf "Failed to submit job: %s\n" msg in
      exit 1
  | Ok job_id ->
      let* () = Lwt_io.printf "✓ Job submitted successfully\n" in
      let* () = Lwt_io.printf "  Job ID: %s\n\n" job_id in
      poll_job_until_done config job_id ~on_terminal_state

let test_slurm_rest () =
  (* Configuration *)
  let base_url = "http://localhost:6820" in
  let username =
    try Sys.getenv "USER" with
    | Not_found -> "unknown"
  in
  (* Generate JWT token *)
  let* () = Lwt_io.printf "Generating JWT token for %s...\n" username in
  let* token_result = Slurm_rest.generate_token ~username () in
  let* token =
    match token_result with
    | Error (`Msg msg) ->
        let* () = Lwt_io.eprintf "Failed to generate token: %s\n" msg in
        exit 1
    | Ok token ->
        let* () = Lwt_io.printf "✓ Token generated\n\n" in
        Lwt.return token
  in
  (* Create client configuration *)
  let config = Slurm_rest.make_config ~base_url ~auth:(Slurm_rest.JWT { token; username }) () in
  (* Test 1: Try submitting a job with invalid feature (should fail) *)
  let* () = Lwt_io.printf "Test 1: Submitting job with invalid feature (riscv64)...\n" in
  let bad_job_spec = make_job_spec ~name:"test-bad-feature" ~script:"#!/bin/bash\necho 'This should fail'" ~constraints:"riscv64" () in
  let* bad_result = Slurm_rest.submit_job config bad_job_spec in
  let* () =
    match bad_result with
    | Error (`Msg msg) -> Lwt_io.printf "✓ Job correctly rejected: %s\n\n" msg
    | Ok job_id -> Lwt_io.printf "✗ Job unexpectedly accepted with ID: %s\n\n" job_id
  in
  (* Test 2: Submit a job that will fail (to check exit code handling) *)
  let* () = Lwt_io.printf "Test 2: Submitting job that will fail (exit 42)...\n" in
  let failing_job_spec = make_job_spec ~name:"test-failure" ~script:"#!/bin/bash\necho 'About to fail...'\nexit 42" () in
  let* () =
    submit_and_poll config failing_job_spec ~on_terminal_state:(fun state job_info ->
        match state with
        | Slurm_rest.Failed ->
            let* () = Lwt_io.printf "\n✓ Job failed as expected\n" in
            let* () =
              match job_info.exit_code with
              | Some 42 -> Lwt_io.printf "✓ Exit code captured correctly: 42\n\n"
              | Some code -> Lwt_io.printf "✗ Wrong exit code: %d (expected 42)\n\n" code
              | None -> Lwt_io.printf "✗ Exit code not available\n\n"
            in
            Lwt.return_unit
        | Slurm_rest.Completed ->
            let* () = Lwt_io.printf "\n✗ Job unexpectedly succeeded\n\n" in
            Lwt.return_unit
        | _ ->
            let* () = Lwt_io.printf "\n✗ Job was cancelled or timed out\n\n" in
            Lwt.return_unit)
  in
  (* Test 3: Submit a valid job *)
  let* () = Lwt_io.printf "Test 3: Submitting valid test job...\n" in
  let job_spec = make_job_spec ~name:"test-hostname" ~script:"#!/bin/bash\nhostname\ndate\nsleep 5\necho 'Job completed'" () in
  let* () =
    submit_and_poll config job_spec ~on_terminal_state:(fun state job_info ->
        match state with
        | Slurm_rest.Completed -> (
            let* () = Lwt_io.printf "\n✓ Job completed successfully!\n" in
            let* () = Lwt_io.printf "\nFull job info:\n" in
            let* () = Lwt_io.printf "  Job ID: %s\n" job_info.job_id in
            let* () = Lwt_io.printf "  Name: %s\n" job_info.name in
            let* () = Lwt_io.printf "  User: %s\n" job_info.user_name in
            let* () = Lwt_io.printf "  State: %s\n" job_info.job_state in
            match job_info.exit_code with
            | Some code -> Lwt_io.printf "  Exit code: %d\n" code
            | None -> Lwt_io.printf "  Exit code: (not set)\n")
        | _ ->
            let* () = Lwt_io.printf "\n✗ Job failed or was cancelled\n" in
            Lwt.return_unit)
  in
  Lwt.return_unit

let () =
  Printf.printf "Testing Slurm REST API client...\n\n";
  Lwt_main.run (test_slurm_rest ())

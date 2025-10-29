open Lwt.Syntax

let log_src = Logs.Src.create "slurm_rest" ~doc:"Slurm REST API client"

module Log = (val Logs.src_log log_src : Logs.LOG)

(** Authentication configuration *)
type auth =
  | JWT of {
      token : string;
      username : string;
    }
  | Unix_socket

type config = {
  base_url : string;
  auth : auth;
  api_version : string;
}
(** Client configuration *)

(** Job state as a variant *)
type job_state =
  | Pending
  | Running
  | Suspended
  | Completed
  | Cancelled
  | Failed
  | Timeout
  | NodeFail
  | Preempted
  | BootFail
  | Deadline
  | OutOfMemory
  | Unknown of string

type job_info = {
  job_id : string;
  job_state : string;
  exit_code : int option; [@yojson.option]
  signal : int option; [@yojson.option]
  name : string;
  user_name : string;
  submit_time : float option; [@yojson.option]
  start_time : float option; [@yojson.option]
  end_time : float option; [@yojson.option]
}
[@@deriving yojson]
(** Job information returned by Slurm *)

type number_object = {
  number : int;
  set : bool;
  infinite : bool;
}
[@@deriving yojson]

type job_object = {
  name : string;
  account : string option; [@yojson.option]
  partition : string option; [@yojson.option]
  nodes : string option; [@yojson.option]
  tasks : int option; [@yojson.option]
  cpus_per_task : int option; [@yojson.option]
  memory_per_node : int option; [@yojson.option]
  time_limit : number_object option; [@yojson.option]
  environment : string list option; [@yojson.option]
  constraints : string option; [@yojson.option]
  current_working_directory : string option; [@yojson.option]
  standard_output : string option; [@yojson.option]
  standard_error : string option; [@yojson.option]
  array : string option; [@yojson.option]
}
[@@deriving yojson]
(** Job object for submission *)

type job_submit_request = {
  script : string;
  job : job_object;
}
[@@deriving yojson]
(** Job submission request *)

type job_submit = {
  name : string;
  script : string;
  account : string option;
  partition : string option;
  nodes : string option;
  tasks : int option;
  cpus_per_task : int option;
  memory_mb : int option;
  time_limit : int option;
  environment : (string * string) list;
  constraints : string option;
  working_directory : string option;
  stdout_path : string option;
  stderr_path : string option;
  array : string option;
}
(** Job submission specification (user-facing) *)

(** Generate a JWT token using scontrol *)
let generate_token ~username ?(lifespan = 3600) () =
  Log.info (fun f -> f "Generating JWT token for user %s" username);
  let cmd = "scontrol" in
  let args = [ cmd; "token"; "username=" ^ username; "lifespan=" ^ string_of_int lifespan ] in
  Lwt.catch
    (fun () ->
      let open Lwt_process in
      let command = (cmd, Array.of_list args) in
      let* status, stdout =
        with_process_in command (fun proc ->
            let* stdout = Lwt_io.read proc#stdout in
            let* status = proc#status in
            Lwt.return (status, stdout))
      in
      match status with
      | Unix.WEXITED 0 -> (
          (* Parse output: SLURM_JWT=eyJ... *)
          let lines = String.split_on_char '\n' stdout in
          let token_line = List.find_opt (fun line -> String.starts_with ~prefix:"SLURM_JWT=" line) lines in
          match token_line with
          | Some line ->
              let token = String.sub line 10 (String.length line - 10) |> String.trim in
              Log.info (fun f -> f "Token generated successfully");
              Lwt.return (Ok token)
          | None ->
              Log.err (fun f -> f "Failed to parse token from scontrol output");
              Lwt.return (Error (`Msg "Failed to parse token from scontrol output")))
      | _ ->
          Log.err (fun f -> f "scontrol token failed: %s" stdout);
          Lwt.return (Error (`Msg ("scontrol token failed: " ^ stdout))))
    (fun exn ->
      Log.err (fun f -> f "Failed to run scontrol: %s" (Printexc.to_string exn));
      Lwt.return (Error (`Msg (Printexc.to_string exn))))

(** Create a client configuration *)
let make_config ~base_url ~auth ?(api_version = "v0.0.40") () = { base_url; auth; api_version }

(** Convert string to job_state variant *)
let job_state_of_string = function
  | "PENDING"
  | "PD" ->
      Pending
  | "RUNNING"
  | "R" ->
      Running
  | "SUSPENDED"
  | "S" ->
      Suspended
  | "COMPLETED"
  | "CD" ->
      Completed
  | "CANCELLED"
  | "CA"
  | "CANCELLED+" ->
      Cancelled
  | "FAILED"
  | "F" ->
      Failed
  | "TIMEOUT"
  | "TO" ->
      Timeout
  | "NODE_FAIL"
  | "NF" ->
      NodeFail
  | "PREEMPTED"
  | "PR" ->
      Preempted
  | "BOOT_FAIL"
  | "BF" ->
      BootFail
  | "DEADLINE"
  | "DL" ->
      Deadline
  | "OUT_OF_MEMORY"
  | "OOM" ->
      OutOfMemory
  | other -> Unknown other

(** Make HTTP request with authentication to /slurm/ endpoints *)
let make_request config ~meth ~path ~body =
  let uri = Uri.of_string (config.base_url ^ "/slurm/" ^ config.api_version ^ path) in
  Log.debug (fun f -> f "%s %s" (Cohttp.Code.string_of_method meth) (Uri.to_string uri));
  (* Build headers with authentication *)
  let headers = Cohttp.Header.init () in
  let headers = Cohttp.Header.add headers "Content-Type" "application/json" in
  let headers = Cohttp.Header.add headers "Accept" "application/json" in
  let headers =
    match config.auth with
    | JWT { token; username } ->
        let headers = Cohttp.Header.add headers "X-SLURM-USER-NAME" username in
        let headers = Cohttp.Header.add headers "X-SLURM-USER-TOKEN" token in
        headers
    | Unix_socket -> headers (* Unix socket uses local user credentials *)
  in
  let body =
    match body with
    | None -> Cohttp_lwt.Body.empty
    | Some json -> Cohttp_lwt.Body.of_string (Yojson.Safe.to_string json)
  in
  Lwt.catch
    (fun () ->
      let* resp, resp_body =
        match meth with
        | `GET -> Cohttp_lwt_unix.Client.get ~headers uri
        | `POST -> Cohttp_lwt_unix.Client.post ~headers ~body uri
        | `DELETE -> Cohttp_lwt_unix.Client.delete ~headers uri
        | _ -> Lwt.fail_with "Unsupported HTTP method"
      in
      let status = Cohttp.Response.status resp in
      let* body_str = Cohttp_lwt.Body.to_string resp_body in
      Log.debug (fun f -> f "Response status: %d" (Cohttp.Code.code_of_status status));
      Log.debug (fun f -> f "Response body: %s" body_str);
      if Cohttp.Code.is_success (Cohttp.Code.code_of_status status) then Lwt.return (Ok body_str)
      else Lwt.return (Error (`Msg (Printf.sprintf "HTTP %d: %s" (Cohttp.Code.code_of_status status) body_str))))
    (fun exn ->
      Log.err (fun f -> f "Request failed: %s" (Printexc.to_string exn));
      Lwt.return (Error (`Msg (Printexc.to_string exn))))

(** Make HTTP request with authentication to /slurmdb/ endpoints *)
let make_request_db config ~meth ~path ~body =
  let uri = Uri.of_string (config.base_url ^ "/slurmdb/" ^ config.api_version ^ path) in
  Log.debug (fun f -> f "%s %s" (Cohttp.Code.string_of_method meth) (Uri.to_string uri));
  (* Build headers with authentication *)
  let headers = Cohttp.Header.init () in
  let headers = Cohttp.Header.add headers "Content-Type" "application/json" in
  let headers = Cohttp.Header.add headers "Accept" "application/json" in
  let headers =
    match config.auth with
    | JWT { token; username } ->
        let headers = Cohttp.Header.add headers "X-SLURM-USER-NAME" username in
        let headers = Cohttp.Header.add headers "X-SLURM-USER-TOKEN" token in
        headers
    | Unix_socket -> headers (* Unix socket uses local user credentials *)
  in
  let body =
    match body with
    | None -> Cohttp_lwt.Body.empty
    | Some json -> Cohttp_lwt.Body.of_string (Yojson.Safe.to_string json)
  in
  Lwt.catch
    (fun () ->
      let* resp, resp_body =
        match meth with
        | `GET -> Cohttp_lwt_unix.Client.get ~headers uri
        | `POST -> Cohttp_lwt_unix.Client.post ~headers ~body uri
        | `DELETE -> Cohttp_lwt_unix.Client.delete ~headers uri
        | _ -> Lwt.fail_with "Unsupported HTTP method"
      in
      let status = Cohttp.Response.status resp in
      let* body_str = Cohttp_lwt.Body.to_string resp_body in
      Log.debug (fun f -> f "Response status: %d" (Cohttp.Code.code_of_status status));
      Log.debug (fun f -> f "Response body: %s" body_str);
      if Cohttp.Code.is_success (Cohttp.Code.code_of_status status) then Lwt.return (Ok body_str)
      else Lwt.return (Error (`Msg (Printf.sprintf "HTTP %d: %s" (Cohttp.Code.code_of_status status) body_str))))
    (fun exn ->
      Log.err (fun f -> f "Request failed: %s" (Printexc.to_string exn));
      Lwt.return (Error (`Msg (Printexc.to_string exn))))

(** Submit a job *)
let submit_job config spec =
  Log.info (fun f -> f "Submitting job: %s" spec.name);
  (* Convert environment from (string * string) list to string list *)
  let environment = if spec.environment = [] then None else Some (List.map (fun (k, v) -> k ^ "=" ^ v) spec.environment) in
  (* Convert time_limit to number_object format *)
  let time_limit =
    match spec.time_limit with
    | None -> None
    | Some minutes -> Some { number = minutes; set = true; infinite = false }
  in
  (* Build job submission request *)
  let request =
    {
      script = spec.script;
      job =
        {
          name = spec.name;
          account = spec.account;
          partition = spec.partition;
          nodes = spec.nodes;
          tasks = spec.tasks;
          cpus_per_task = spec.cpus_per_task;
          memory_per_node = spec.memory_mb;
          time_limit;
          environment;
          constraints = spec.constraints;
          current_working_directory = spec.working_directory;
          standard_output = spec.stdout_path;
          standard_error = spec.stderr_path;
          array = spec.array;
        };
    }
  in
  (* Convert to JSON using ppx_deriving *)
  let request_body = job_submit_request_to_yojson request in
  let* result = make_request config ~meth:`POST ~path:"/job/submit" ~body:(Some request_body) in
  match result with
  | Error e -> Lwt.return (Error e)
  | Ok body_str -> (
      try
        let json = Yojson.Safe.from_string body_str in
        let open Yojson.Safe.Util in
        (* Slurm REST API returns job_id at top level *)
        let job_id = json |> member "job_id" |> to_int |> string_of_int in
        Log.info (fun f -> f "Job submitted: %s" job_id);
        Lwt.return (Ok job_id)
      with
      | exn ->
          Log.err (fun f -> f "Failed to parse submit response: %s" (Printexc.to_string exn));
          Lwt.return (Error (`Msg ("Failed to parse submit response: " ^ body_str))))

(** Parse job info from JSON *)
let parse_job_info json =
  try
    let open Yojson.Safe.Util in
    let job_id = json |> member "job_id" |> to_int |> string_of_int in
    (* job_state is an array like ["PENDING"] *)
    let job_state = json |> member "job_state" |> to_list |> List.hd |> to_string in
    let name = json |> member "name" |> to_string in
    let user_name = json |> member "user_name" |> to_string in
    (* Parse exit_code - in {set, infinite, number} format *)
    let exit_code =
      try
        let obj = json |> member "exit_code" |> member "return_code" in
        let is_set = obj |> member "set" |> to_bool in
        if is_set then Some (obj |> member "number" |> to_int) else None
      with
      | _ -> None
    in
    (* Parse signal - also in {set, infinite, number} format *)
    let signal =
      try
        let obj = json |> member "exit_code" |> member "signal" |> member "id" in
        let is_set = obj |> member "set" |> to_bool in
        if is_set then Some (obj |> member "number" |> to_int) else None
      with
      | _ -> None
    in
    (* Timestamps are in {set, infinite, number} format *)
    let submit_time =
      try
        let obj = json |> member "submit_time" in
        let num = obj |> member "number" |> to_int in
        Some (float_of_int num)
      with
      | _ -> None
    in
    let start_time =
      try
        let obj = json |> member "start_time" in
        let num = obj |> member "number" |> to_int in
        Some (float_of_int num)
      with
      | _ -> None
    in
    let end_time =
      try
        let obj = json |> member "end_time" in
        let num = obj |> member "number" |> to_int in
        Some (float_of_int num)
      with
      | _ -> None
    in
    Some { job_id; job_state; exit_code; signal; name; user_name; submit_time; start_time; end_time }
  with
  | exn ->
      Log.warn (fun f -> f "Failed to parse job info: %s" (Printexc.to_string exn));
      None

(** Get job information *)
let get_job config job_id =
  Log.debug (fun f -> f "Getting job info: %s" job_id);
  let* result = make_request config ~meth:`GET ~path:("/job/" ^ job_id) ~body:None in
  match result with
  | Error e -> Lwt.return (Error e)
  | Ok body_str -> (
      try
        let json = Yojson.Safe.from_string body_str in
        let open Yojson.Safe.Util in
        let jobs = json |> member "jobs" |> to_list in
        match jobs with
        | [] -> Lwt.return (Error (`Msg "Job not found"))
        | job_json :: _ -> (
            match parse_job_info job_json with
            | Some job_info -> Lwt.return (Ok job_info)
            | None -> Lwt.return (Error (`Msg "Failed to parse job info")))
      with
      | exn ->
          Log.err (fun f -> f "Failed to parse job response: %s" (Printexc.to_string exn));
          Lwt.return (Error (`Msg ("Failed to parse job response: " ^ body_str))))

(** Get multiple jobs information *)
let get_jobs config ?job_ids () =
  Log.debug (fun f -> f "Getting jobs info");
  let path =
    match job_ids with
    | None -> "/jobs"
    | Some ids -> "/jobs?job_id=" ^ String.concat "," ids
  in
  let* result = make_request config ~meth:`GET ~path ~body:None in
  match result with
  | Error e -> Lwt.return (Error e)
  | Ok body_str -> (
      try
        let json = Yojson.Safe.from_string body_str in
        let open Yojson.Safe.Util in
        let jobs = json |> member "jobs" |> to_list in
        let job_infos = List.filter_map parse_job_info jobs in
        Lwt.return (Ok job_infos)
      with
      | exn ->
          Log.err (fun f -> f "Failed to parse jobs response: %s" (Printexc.to_string exn));
          Lwt.return (Error (`Msg ("Failed to parse jobs response: " ^ body_str))))

(** Cancel a job *)
let cancel_job config job_id =
  Log.info (fun f -> f "Cancelling job: %s" job_id);
  let* result = make_request config ~meth:`DELETE ~path:("/job/" ^ job_id) ~body:None in
  match result with
  | Error e -> Lwt.return (Error e)
  | Ok _ ->
      Log.info (fun f -> f "Job cancelled: %s" job_id);
      Lwt.return (Ok ())

(** Parse job info from slurmdb JSON (different format than /slurm/ endpoint) *)
let parse_job_info_db json =
  try
    let open Yojson.Safe.Util in
    let job_id = json |> member "job_id" |> to_int |> string_of_int in
    (* In slurmdb, job_state is nested: state.current is an array like ["NODE_FAIL"] *)
    let job_state = json |> member "state" |> member "current" |> to_list |> List.hd |> to_string in
    let name = json |> member "name" |> to_string in
    let user = json |> member "user" |> to_string in
    (* Exit code *)
    let exit_code =
      try
        let obj = json |> member "exit_code" |> member "return_code" in
        let is_set = obj |> member "set" |> to_bool in
        if is_set then Some (obj |> member "number" |> to_int) else None
      with
      | _ -> None
    in
    (* Signal *)
    let signal =
      try
        let obj = json |> member "exit_code" |> member "signal" |> member "id" in
        let is_set = obj |> member "set" |> to_bool in
        if is_set then Some (obj |> member "number" |> to_int) else None
      with
      | _ -> None
    in
    (* Timestamps *)
    let submit_time =
      try
        let obj = json |> member "time" |> member "submission" in
        let num = obj |> member "number" |> to_int in
        Some (float_of_int num)
      with
      | _ -> None
    in
    let start_time =
      try
        let obj = json |> member "time" |> member "start" in
        let num = obj |> member "number" |> to_int in
        Some (float_of_int num)
      with
      | _ -> None
    in
    let end_time =
      try
        let obj = json |> member "time" |> member "end" in
        let num = obj |> member "number" |> to_int in
        Some (float_of_int num)
      with
      | _ -> None
    in
    Some { job_id; job_state; exit_code; signal; name; user_name = user; submit_time; start_time; end_time }
  with
  | exn ->
      Log.warn (fun f -> f "Failed to parse slurmdb job info: %s" (Printexc.to_string exn));
      None

(** Get jobs from slurmdb (includes completed/archived jobs) *)
let get_jobs_db config ?users () =
  Log.debug (fun f -> f "Getting jobs from slurmdb");
  let path =
    match users with
    | None -> "/jobs"
    | Some user_list -> "/jobs?users=" ^ String.concat "," user_list
  in
  let* result = make_request_db config ~meth:`GET ~path ~body:None in
  match result with
  | Error e -> Lwt.return (Error e)
  | Ok body_str -> (
      try
        let json = Yojson.Safe.from_string body_str in
        let open Yojson.Safe.Util in
        let jobs = json |> member "jobs" |> to_list in
        let job_infos = List.filter_map parse_job_info_db jobs in
        Lwt.return (Ok job_infos)
      with
      | exn ->
          Log.err (fun f -> f "Failed to parse slurmdb jobs response: %s" (Printexc.to_string exn));
          Lwt.return (Error (`Msg ("Failed to parse slurmdb jobs response: " ^ body_str))))

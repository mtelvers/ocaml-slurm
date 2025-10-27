(** Slurm REST API client library *)

(** Authentication configuration *)
type auth =
  | JWT of {
      token : string;
      username : string;
    }  (** JWT token with username *)
  | Unix_socket  (** Unix socket (uses local user credentials) *)

type config = {
  base_url : string;  (** Base URL, e.g., "http://localhost:6820" or "unix:///var/run/slurmrestd.sock" *)
  auth : auth;
  api_version : string;  (** API version, e.g., "v0.0.40" *)
}
(** Client configuration *)

(** Job state from Slurm *)
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
  job_state : string;  (** Job state as string from Slurm *)
  exit_code : int option;  (** Process exit code *)
  signal : int option;  (** Signal that killed the process (if any) *)
  name : string;
  user_name : string;
  submit_time : float option;
  start_time : float option;
  end_time : float option;
}
(** Job information returned by Slurm *)

type job_submit = {
  name : string;
  script : string;  (** Job script content *)
  account : string option;
  partition : string option;
  nodes : string option;  (** Number of nodes (string format, e.g., "1", "2-4") *)
  tasks : int option;
  cpus_per_task : int option;
  memory_mb : int option;
  time_limit : int option;  (** Time limit in minutes *)
  environment : (string * string) list;
  constraints : string option;
  working_directory : string option;
  stdout_path : string option;
  stderr_path : string option;
}
(** Job submission specification *)

(** Generate a JWT token using scontrol *)
val generate_token : username:string -> ?lifespan:int -> unit -> (string, [> `Msg of string ]) result Lwt.t
(** Generate a JWT token using scontrol command. Lifespan in seconds (default: 3600) *)

val make_config : base_url:string -> auth:auth -> ?api_version:string -> unit -> config
(** Create a client configuration *)

val job_state_of_string : string -> job_state
(** Convert string to job_state variant *)

(** Submit a job *)
val submit_job : config -> job_submit -> (string, [> `Msg of string ]) result Lwt.t
(** Returns job ID on success *)

val get_job : config -> string -> (job_info, [> `Msg of string ]) result Lwt.t
(** Get job information *)

val get_jobs : config -> ?job_ids:string list -> unit -> (job_info list, [> `Msg of string ]) result Lwt.t
(** Get multiple jobs information *)

val cancel_job : config -> string -> (unit, [> `Msg of string ]) result Lwt.t
(** Cancel a job *)

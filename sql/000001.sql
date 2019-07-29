BEGIN;

-- Create the tables
create table assemble_worker.job_queues (
  queue_name text not null primary key,
  created boolean default false
);

create table assemble_worker.pending_jobs (
  queue_name text not null,
  payload json default '{}'::json not null,
  max_attempts int default 25 not null,
  created_at timestamp with time zone not null default now()
);

create index queue_name_idx on assemble_worker.pending_jobs (queue_name);

create table assemble_worker.jobs (
  id bigserial primary key,
  queue_name text not null,
  payload json default '{}'::json not null,
  attempts int default 0 not null,
  max_attempts int default 25 not null,
  errors text[] default ARRAY[]::text[],
  ran_at timestamp[] default ARRAY[]::text[],
  created_at timestamp not null default now()
);

-- Notify worker of new queues to be created in Rabbit
create function assemble_worker.tg_job_queues__notify_new_queues() returns trigger as $$
begin
  perform pg_notify('assemble-worker', 'meta:new-queue' || '|', NEW.queue_name)
  return NEW;
end;
$$ language plpgsql;

-- When a queue has been created, send pending jobs for that queue to jobs 
create function assemble_worker.tg_job_queues__after_queue_create() returns trigger as $$
begin
  create temp table assemble_worker.pending_jobs_to_queue as
  select payload, created_at, max_attempts
  from assemble_worker.pending_jobs
  where queue_name = NEW.queue_name;

  insert into assemble_worker.jobs (queue_name, payload, created_at, max_attempts)
  select NEW.queue_name as queue_name, payload, created_at, max_attempts
  from assemble_worker.pending_jobs_to_queue;

  delete from assemble_worker.pending_jobs_to_queue
  where queue_name = NEW.queue_name;
end;
$$ language plpgsql;

create trigger _500_queue_pending_jobs after update on assemble_worker.job_queues when (OLD.created = false and NEW.created = true) for each statement execute procedure assemble_worker.tg_job_queues__after_queue_create();

-- Notify worker of new jobs
create function assemble_worker.tg_jobs__notify_new_jobs() returns trigger as $$
begin
  perform pg_notify('assemble-worker', NEW.queue_name || '|' || payload::text);
  return NEW;
end;
$$ language plpgsql;

create trigger _900_notify_worker after insert on assemble_worker.jobs for each statement execute procedure assemble_worker.tg_jobs__notify_new_jobs();

-- Function to queue a job - put it into pending_jobs if the queue does not exist yet
create function assemble_worker.add_job(job_name text, payload json = '{}', max_attempts int = 25) returns void as $$
declare
  v_queue_exists boolean;
  v_result 
begin
  select exists (
    select 1
    from assemble.job_queues
    where queue_name = job_name
      and created = true;
  ) into v_queue_exists;

  if v_queue_exists then
    insert into assemble_worker.jobs (job_name, payload, max_attempts)
    values (job_name, payload, max_attempts);
  else if
    insert into assemble_worker.jobs (job_name, payload, max_attempts)
    values (job_name, payload, max_attempts);

    insert into assemble.job_queues (queue_name)
    values (job_name)
    on conflict (queue_name) do nothing; 
  end if;
end;
$$ language plpgsql;

-- I was successful, mark the job as completed
create function assemble_worker.complete_job(job_id bigint) returns assemble_worker.jobs as $$
declare
  v_row assemble_worker.jobs;
begin
  delete from assemble_worker.jobs
    where id = job_id
    returning * into v_row;
  return v_row;
end;
$$ language plpgsql;

-- I was unsuccessful, record the failure – it will be retried via Rabbit
create function assemble_worker.fail_job(job_id bigint, error_message text) returns assemble_worker.jobs as $$
declare
  v_row assemble_worker.jobs;
begin
  update assemble_worker.jobs
    set
      errors = array_append(errors, error_message),
      ran_at = array_append(ran_at, now())
    where id = job_id
    returning * into v_row;
  return v_row;
end;
$$ language plpgsql;

COMMIT;

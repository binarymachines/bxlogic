CREATE TABLE "boroughs" (
  "id" int4 NOT NULL,
  "value" varchar(16) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "clients" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "first_name" varchar(64) NOT NULL,
  "last_name" varchar(64),
  "phone" varchar(16) NOT NULL,
  "email" varchar(64),
  PRIMARY KEY ("id")
);

CREATE TABLE "courier_boroughs" (
  "courier_id" uuid NOT NULL,
  "borough_id" int4 NOT NULL,
  PRIMARY KEY ("courier_id", "borough_id")
);

CREATE TABLE "courier_transport_methods" (
  "courier_id" uuid NOT NULL,
  "transport_method_id" int4 NOT NULL,
  PRIMARY KEY ("courier_id", "transport_method_id")
);

CREATE TABLE "couriers" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "first_name" varchar(32) NOT NULL,
  "last_name" varchar(32) NOT NULL,
  "mobile_number" varchar(32) NOT NULL,
  "email" varchar(64) NOT NULL,
  "duty_status" int2 NOT NULL,
  "deleted_ts" timestamp(255),
  PRIMARY KEY ("id")
);

CREATE TABLE "job_assignments" (
  "courier_id" uuid NOT NULL,
  "job_id" uuid NOT NULL,
  "job_tag" varchar(64) NOT NULL,
  "phase" int2,
  PRIMARY KEY ("courier_id", "job_id")
);

CREATE TABLE "job_bids" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "job_tag" varchar(128) NOT NULL,
  "courier_id" uuid NOT NULL,
  "write_ts" timestamp NOT NULL,
  "accepted_ts" timestamp,
  "expired_ts" timestamp,
  PRIMARY KEY ("id")
);

CREATE TABLE "job_data" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "client_id" uuid NOT NULL,
  "job_tag" varchar(255) NOT NULL,
  "delivery_address" varchar(128) NOT NULL,
  "delivery_borough" varchar(16) NOT NULL,
  "delivery_zip" varchar(10) NOT NULL,
  "delivery_neighborhood" varchar(64),
  "pickup_address" varchar(128) NOT NULL,
  "pickup_borough" varchar(16) NOT NULL,
  "pickup_neighborhood" varchar(64),
  "pickup_zip" varchar(10),
  "payment_method" int2 NOT NULL,
  "items" text,
  "delivery_window_open" timestamp(255),
  "delivery_window_close" timestamp(255),
  "deleted_ts" timestamp(255),
  PRIMARY KEY ("id")
);

CREATE TABLE "job_logs" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "job_tag" varchar(255) NOT NULL,
  "data" text NOT NULL,
  "log_time" timestamp,
  PRIMARY KEY ("id")
);

CREATE TABLE "job_status" (
  "id" uuid NOT NULL DEFAULT public.uuid_generate_v4(),
  "job_tag" varchar(128) NOT NULL,
  "status" int2 NOT NULL,
  "write_ts" timestamp(255) NOT NULL,
  "expired_ts" timestamp(255),
  PRIMARY KEY ("id")
);

CREATE TABLE "lookup_duty_status" (
  "id" int4 NOT NULL,
  "value" varchar(16) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "lookup_job_phases" (
  "id" int4 NOT NULL,
  "value" varchar(16) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "lookup_job_status" (
  "id" int4 NOT NULL,
  "value" varchar(16) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "lookup_payment_methods" (
  "id" int4 NOT NULL,
  "value" varchar(32) NOT NULL,
  PRIMARY KEY ("id")
);

CREATE TABLE "transport_methods" (
  "id" int4 NOT NULL,
  "value" varchar(16) NOT NULL,
  PRIMARY KEY ("id")
);

ALTER TABLE "courier_boroughs" ADD CONSTRAINT "fk_courier_boroughs_couriers_1" FOREIGN KEY ("courier_id") REFERENCES "couriers" ("id");
ALTER TABLE "courier_boroughs" ADD CONSTRAINT "fk_courier_boroughs_boroughs_1" FOREIGN KEY ("borough_id") REFERENCES "boroughs" ("id");
ALTER TABLE "courier_transport_methods" ADD CONSTRAINT "fk_courier_transport_methods_transport_methods_1" FOREIGN KEY ("transport_method_id") REFERENCES "transport_methods" ("id");
ALTER TABLE "courier_transport_methods" ADD CONSTRAINT "fk_courier_transport_methods_couriers_1" FOREIGN KEY ("courier_id") REFERENCES "couriers" ("id");
ALTER TABLE "job_assignments" ADD CONSTRAINT "fk_job_assignments_couriers_1" FOREIGN KEY ("courier_id") REFERENCES "couriers" ("id");
ALTER TABLE "job_assignments" ADD CONSTRAINT "fk_job_assignments_job_data_1" FOREIGN KEY ("job_id") REFERENCES "job_data" ("id");
ALTER TABLE "job_bids" ADD CONSTRAINT "fk_job_bids_couriers_1" FOREIGN KEY ("courier_id") REFERENCES "couriers" ("id");
ALTER TABLE "job_data" ADD CONSTRAINT "fk_job_data_clients_1" FOREIGN KEY ("client_id") REFERENCES "clients" ("id");


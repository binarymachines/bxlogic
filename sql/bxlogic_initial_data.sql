
-- starting values for BXLOGIC database (lookup tables)

INSERT INTO lookup_job_status
(id, value)
VALUES 
(0, 'broadcast'),
(1, 'accepted_partial'),
(2, 'accepted'),
(3, 'in_progress'),
(4, 'completed'),
(6, 'canceled');


INSERT INTO lookup_job_phases
(id, value)
VALUES
(1, 'pick'),
(2, 'deliver')



INSERT INTO lookup_duty_status
(id, value)
VALUES
(0, 'inactive'),
(1, 'active');


INSERT INTO transport_methods
(id, value)
VALUES
(1, 'Bicycle'),
(2, 'Car'),
(3, 'Motorcycle'),
(4, 'Walking');


INSERT INTO boroughs
(id, value)
VALUES
(1, 'Brooklyn'),
(2, 'Queens'),
(3, 'Manhattan'),
(4, 'The Bronx'),
(5, 'Staten Island');


INSERT INTO lookup_payment_methods
(id, value)
VALUES
(1, 'Cash'),
(2, 'PayPal'),
(3, 'Venmo'),
(4, 'CashApp'),
(5, 'Zelle'),
(6, 'AbolitionAction')

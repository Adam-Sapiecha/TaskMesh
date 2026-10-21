-- Optional public visitor sessions. Private jobs retain a NULL scope.
CREATE TABLE public_sessions (
    token_hash text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL DEFAULT clock_timestamp()+interval '24 hours',
    submitted integer NOT NULL DEFAULT 0 CHECK (submitted BETWEEN 0 AND 30)
);
CREATE INDEX public_sessions_expiry ON public_sessions(expires_at);
ALTER TABLE jobs ADD COLUMN public_session_hash text REFERENCES public_sessions(token_hash);
CREATE INDEX jobs_public_session ON jobs(public_session_hash,created_at DESC) WHERE public_session_hash IS NOT NULL;
CREATE TABLE public_budget (
    id integer PRIMARY KEY CHECK (id=1),
    window_start timestamptz NOT NULL DEFAULT date_trunc('hour',clock_timestamp()),
    sessions integer NOT NULL DEFAULT 0,
    jobs integer NOT NULL DEFAULT 0
);
INSERT INTO public_budget(id) VALUES(1);

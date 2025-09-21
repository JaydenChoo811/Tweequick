-- Towns Data by MET
CREATE TABLE IF NOT EXISTS public.towns
(
    id text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    latitude double precision,
    longitude double precision,
    state_id text COLLATE pg_catalog."default",
    district_id text COLLATE pg_catalog."default",
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT towns_pkey PRIMARY KEY (id),
    CONSTRAINT towns_district_id_fkey FOREIGN KEY (district_id)
        REFERENCES public.districts (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT towns_state_id_fkey FOREIGN KEY (state_id)
        REFERENCES public.states (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
)

-- Districts Data by MET
CREATE TABLE IF NOT EXISTS public.districts
(
    id text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    state_id text COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT districts_pkey PRIMARY KEY (id),
    CONSTRAINT districts_state_id_fkey FOREIGN KEY (state_id)
        REFERENCES public.states (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)

-- States Data by MET
CREATE TABLE IF NOT EXISTS public.states
(
    id text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT states_pkey PRIMARY KEY (id)
)

-- Table 1: Raw tweets data
CREATE TABLE tweets (
    id SERIAL PRIMARY KEY,
    tweet_text VARCHAR(500) NOT NULL,
    tweet_timestamp TIMESTAMP NOT NULL,
    mock_location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 2: NLP analysis results
CREATE TABLE analysis_results (
    id SERIAL PRIMARY KEY,
    tweet_id INTEGER REFERENCES tweets(id),
    flood_detected BOOLEAN NOT NULL,
    urgency_score INTEGER CHECK (urgency_score >= 1 AND urgency_score <= 10),
    extracted_state VARCHAR(50),
    extracted_city VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Weather and meteorological data
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    tweet_id INTEGER REFERENCES tweets(id),
    district VARCHAR(100),
    warning_level INTEGER CHECK (warning_level >= 0 AND warning_level <= 4),
    rainfall_mm DECIMAL(5,2),
    temperature_celsius DECIMAL(4,1),
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: Final risk calculations
CREATE TABLE risk_scores (
    id SERIAL PRIMARY KEY,
    tweet_id INTEGER REFERENCES tweets(id),
    final_score DECIMAL(3,1) CHECK (final_score >= 1 AND final_score <= 10),
    risk_level VARCHAR(20) CHECK (risk_level IN ('Low', 'Moderate', 'High', 'Critical')),
    recommendation TEXT,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tweets_timestamp ON tweets(tweet_timestamp);
CREATE INDEX idx_analysis_tweet_id ON analysis_results(tweet_id);
CREATE INDEX idx_weather_tweet_id ON weather_data(tweet_id);
CREATE INDEX idx_risk_tweet_id ON risk_scores(tweet_id);
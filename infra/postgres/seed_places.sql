-- Day 6 CDC 데모용 places 마스터.
-- replica identity FULL → DELETE/UPDATE 시 이전 값까지 Debezium 으로 흐른다.

CREATE TABLE IF NOT EXISTS places (
    place_id        BIGSERIAL PRIMARY KEY,
    biz_reg_no      TEXT UNIQUE,                 -- 사업자등록번호 (정규)
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,               -- '카페', '음식점', '편의점' 등
    district        TEXT NOT NULL,
    gu_code         TEXT NOT NULL,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    open_hour       INTEGER,                     -- 0~23
    close_hour      INTEGER,                     -- 0~23, close_hour < open_hour 이면 자정 넘김
    status          TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'closed'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE places REPLICA IDENTITY FULL;

CREATE OR REPLACE FUNCTION places_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_places_updated_at ON places;
CREATE TRIGGER trg_places_updated_at
    BEFORE UPDATE ON places
    FOR EACH ROW
    EXECUTE FUNCTION places_touch_updated_at();

-- Day 6 데모용 시드 (자치구는 Week 1 hotspot_regions.csv 와 일치)
INSERT INTO places (biz_reg_no, name, category, district, gu_code, latitude, longitude, open_hour, close_hour)
VALUES
    ('1208612345', '강남 모닝카페', '카페', '강남구', '11680', 37.4985, 127.0280, 7, 23),
    ('1208612346', '홍대 24시 분식', '음식점', '마포구', '11440', 37.5572, 126.9242, 0, 24),
    ('1208612347', '여의도 브런치', '카페', '영등포구', '11560', 37.5215, 126.9248, 9, 21),
    ('1208612348', '종로 한정식', '음식점', '종로구', '11110', 37.5703, 126.9915, 11, 22),
    ('1208612349', '성수 로스터리', '카페', '성동구', '11200', 37.5450, 127.0560, 8, 23)
ON CONFLICT (biz_reg_no) DO NOTHING;

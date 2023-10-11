/*********************************
Find tracks by deadmau5
- Released within the past 3 years
- With 'House' in the genre name
- Original mixes only (no remixes)
*********************************/
SELECT bp__artist.artist_name
    , bp__track.title
    , bp__track.mix
    , bp__genre.genre_name
    , bp__track.bpm
    , bp__track.release_date
FROM bp__artist_track
JOIN bp__artist ON bp__artist.artist_id = bp__artist_track.artist_id
JOIN bp__track ON bp__track.track_id = bp__artist_track.track_id
JOIN bp__genre ON bp__genre.genre_id = bp__track.genre_id
WHERE bp__artist.artist_name = 'deadmau5'
    AND bp__track.release_date > dateadd(year, -3, getdate())
    AND bp__genre.genre_name LIKE '%House%'
    AND bp__track.is_remixed <> 'True'
create table if not exists fabric_release_blacklist (
    release_id integer not null unique,
    foreign key (release_id) references release(id)
);

insert into fabric_release_blacklist values
   (8646406),
   (6133380),
   (2042298),
   (15289518),
   (8476206),
   (477868),
   (477838),
   (15354807),
   (15456306)
   ;

create materialized view if not exists fabric_releases as
       (select 'fabric' as fabric_series, release.id as release_id, master.id as master_id, release.title,
               (regexp_matches(release.title, 'Fabric\.? ?(\d+)', 'gi'))[1]::int as fabric_num
           from release, master where release.title ~* 'fabric\.? ?\d+' and
	   not release.title ~* 'Radio Mix' and
	   release.master_id is not null and
	   release.status = 'Accepted' and
	   not release.title ~* 'Sampler' and
	   not release.title ~* 'Syndicated' and
	   release.id not in (select * from fabric_release_blacklist) and
           master.id = release.master_id and release.id = master.main_release)
        union
       (select 'fabric' as fabric_series, release.id as release_id, release.master_id as master_id, release.title,
               (regexp_matches(release.title, 'Fabric\.? ?(\d+)', 'gi'))[1]::int as fabric_num
        from release where release.title ~* 'fabric\.? ?\d+' and
	   not release.title ~* 'Radio Mix' and
	   release.status = 'Accepted' and
	   not release.title ~* 'Sampler' and
	   not release.title ~* 'Syndicated' and
	   release.id not in (select * from fabric_release_blacklist) and
           release.master_id is null)
	union
       (select 'fabriclive' as fabric_series, release.id as release_id, master.id as master_id, release.title,
               (regexp_matches(release.title, 'FabricLive\.? ?(\d+)', 'gi'))[1]::int as fabric_num
        from release, master where release.title ~* 'fabriclive\.? ?\d+' and
	not release.title ~* 'Radio Mix' and
	release.status = 'Accepted' and
	not release.title ~* 'Sampler' and
	not release.title ~* 'Syndicated' and
	release.id not in (select * from fabric_release_blacklist) and
        master.id = release.master_id and release.id = master.main_release)
       union
       (select 'fabriclive' as fabric_series, release.id as release_id, release.master_id as master_id, release.title,
               (regexp_matches(release.title, 'FabricLive\.? ?(\d+)', 'gi'))[1]::int as fabric_num
        from release where release.title ~* 'fabriclive\.? ?\d+' and
	not release.title ~* 'Radio Mix' and
	release.status = 'Accepted' and
	not release.title ~* 'Sampler' and
	not release.title ~* 'Syndicated' and
	release.id not in (select * from fabric_release_blacklist) and
        release.master_id is null);

create materialized view if not exists fabric_release_artists as
select
fabric_releases.release_id,
fabric_num,
fabric_series,
artist_id,
artist_name,
anv
from fabric_releases left outer join release_artist
on (fabric_releases.release_id = release_artist.release_id)
where not extra::bool;

create materialized view if not exists fabric_tracks as
select
fabric_releases.fabric_num as fabric_num,
fabric_releases.fabric_series as fabric_series,
fabric_releases.release_id as release_id,
release.title as release_title,
release_track.id as track_id,
release_track.title as track_title,
release_track.sequence as track_sequence,
release_track.position as track_position
from fabric_releases, release, release_track
where fabric_releases.release_id is not null
and fabric_releases.release_id = release.id
and release_track.release_id = release.id;

create materialized view if not exists fabric_tracks_artists as
select
fabric_tracks.track_id as track_id,
array_agg(concat_ws(' ', concat_ws(':', format('[%s]', anv), format('[%s]', artist_name)), join_string)  order by position) as track_artists
from fabric_tracks left join release_track_artist on (fabric_tracks.track_id = release_track_artist.track_id)
where not release_track_artist.extra
and fabric_tracks.track_position is not null
group by fabric_tracks.track_id, release_track_artist.track_sequence;


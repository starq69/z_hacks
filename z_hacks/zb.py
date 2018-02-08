bundles = {'test': 'ready'}    #####

def ingest(name='test',
           environ=os.environ,
           timestamp=None,
           assets_versions=(),
           show_progress=False):

    try:
        bundle = bundles[name]
    except KeyError:
        raise UnknownBundle(name)
    """
    calendar = get_calendar(bundle.calendar_name)

    start_session = bundle.start_session
    end_session = bundle.end_session

    if start_session is None or start_session < calendar.first_session:
        start_session = calendar.first_session

    if end_session is None or end_session > calendar.last_session:
        end_session = calendar.last_session

    if timestamp is None:
        timestamp = pd.Timestamp.utcnow()
    timestamp = timestamp.tz_convert('utc').tz_localize(None)
    timestr = to_bundle_ingest_dirname(timestamp)
    cachepath = cache_path(name, environ=environ)
    pth.ensure_directory(pth.data_path([name, timestr], environ=environ))
    pth.ensure_directory(cachepath)
    """


    with dataframe_cache(cachepath, clean_on_failure=False) as cache, ExitStack() as stack:

        if bundle.create_writers:

            print('bundle.create_writers : OK')
            '''
            wd = stack.enter_context(working_dir(
                pth.data_path([], environ=environ))
            )
            daily_bars_path = wd.ensure_dir(
                *daily_equity_relative(
                    name, timestr, environ=environ,
                )
            )
            daily_bar_writer = BcolzDailyBarWriter(
                daily_bars_path,
                calendar,
                start_session,
                end_session,
            )
            '''
            # Do an empty write to ensure that the daily ctables exist
            # when we create the SQLiteAdjustmentWriter below. The
            # SQLiteAdjustmentWriter needs to open the daily ctables so
            # that it can compute the adjustment ratios for the dividends.

            daily_bar_writer.write(())
            minute_bar_writer = BcolzMinuteBarWriter(
                wd.ensure_dir(*minute_equity_relative(
                    name, timestr, environ=environ)
                ),
                calendar,
                start_session,
                end_session,
                minutes_per_day=bundle.minutes_per_day,
            )
            assets_db_path = wd.getpath(*asset_db_relative(
                name, timestr, environ=environ,
            ))
            asset_db_writer = AssetDBWriter(assets_db_path)

            adjustment_db_writer = stack.enter_context(
                SQLiteAdjustmentWriter(
                    wd.getpath(*adjustment_db_relative(
                        name, timestr, environ=environ)),
                    BcolzDailyBarReader(daily_bars_path),
                    calendar.all_sessions,
                    overwrite=True,
                )
            )
        else:
            daily_bar_writer = None
            minute_bar_writer = None
            asset_db_writer = None
            adjustment_db_writer = None
            if assets_versions:
                raise ValueError('Need to ingest a bundle that creates '
                                 'writers in order to downgrade the assets'
                                 ' db.')
        bundle.ingest(
            environ,
            asset_db_writer,
            minute_bar_writer,
            daily_bar_writer,
            adjustment_db_writer,
            calendar,
            start_session,
            end_session,
            cache,
            show_progress,
            pth.data_path([name, timestr], environ=environ),
        )

        for version in sorted(set(assets_versions), reverse=True):
            version_path = wd.getpath(*asset_db_relative(
                name, timestr, environ=environ, db_version=version,
            ))
            with working_file(version_path) as wf:
                shutil.copy2(assets_db_path, wf.path)
                downgrade(wf.path, version)


#
#def _fetch_raw_metadata(api_key, cache, retries, environ):
def _fetch_raw_metadata(api_key, cache, retries=5):
    """Generator that yields each page of data from the metadata endpoint
    as a dataframe.
    """
    for page_number in count(1):
        key = 'metadata-page-%d' % page_number
        try:
            raw = cache[key]
        except KeyError:
            for _ in range(retries):
                try:
                    raw = pd.read_csv(
                        format_metadata_url(api_key, page_number),
                        parse_dates=[
                            'oldest_available_date',
                            'newest_available_date',
                        ],
                        usecols=[
                            'dataset_code',
                            'name',
                            'oldest_available_date',
                            'newest_available_date',
                        ],
                    )
                    break
                except ValueError:
                    # when we are past the last page we will get a value
                    # error because there will be no columns
                    raw = pd.DataFrame([])
                    break
                except Exception:
                    pass
            else:
                raise ValueError(
                    'Failed to download metadata page %d after %d'
                    ' attempts.' % (page_number, retries),
                )

            cache[key] = raw 

        if raw.empty:
            # use the empty dataframe to signal completion
            break
        yield raw 


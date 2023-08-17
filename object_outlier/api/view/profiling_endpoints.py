from flask import request, jsonify, current_app, Response, render_template, g, session, Blueprint, redirect


def create_profiling_endpoint(app, profiling_service, bp_profiling):
    ps = profiling_service

    @bp_profiling.route("/regex", methods=['POST'])
    def profiling_regex():
        print('/regex')
        payload = request.get_json(force=True)
        return ps.profiling_regex(payload=payload)

    @bp_profiling.route("/1", methods=['POST'])
    def get_dataframe_describe():
        payload = request.get_json(force=True)
        return ps.get_dataframe_describe(payload=payload)

    @bp_profiling.route("/2", methods=['POST'])
    def get_pandas_profiling_describe():
        payload = request.get_json(force=True)
        return ps.get_pandas_profiling_describe(payload=payload)

    @bp_profiling.route("/3", methods=['POST'])
    def get_pandas_profiling_iframe():
        payload = request.get_json(force=True)
        return ps.get_pandas_profiling_iframe(payload=payload)

    @bp_profiling.route("/column", methods=['POST'])
    def get_profiling_column():
        payload = request.get_json(force=True)
        return ps.get_profiling(payload=payload)

    @bp_profiling.route("/dataset", methods=['POST'])
    def get_profiling_dataset():
        payload = request.get_json(force=True)
        return ps.get_profiling(payload=payload)

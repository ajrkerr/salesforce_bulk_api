require 'rubygems'
require 'bundler'
Bundler.require()
require 'salesforce_bulk_api/version'
require 'net/https'
require 'xmlsimple'
require 'csv'
require 'salesforce_bulk_api/concerns/throttling'
require 'salesforce_bulk_api/job'
require 'salesforce_bulk_api/connection'

module SalesforceBulkApi

  class Api
    attr_reader :connection

    @@SALESFORCE_API_VERSION = '32.0'
    DEFAULT_TIMEOUT = 1500
    DEFAULT_BATCH_SIZE = 10000

    def initialize(client)
      @connection = SalesforceBulkApi::Connection.new(@@SALESFORCE_API_VERSION, client)
      @listeners = { job_created: [] }
    end

    def upsert(sobject, records, external_key, options = {})
      do_operation('upsert', sobject, records, options.merge(external_key: external_key))
    end

    def update(sobject, records, options ={})
      do_operation('update', sobject, records, options)
    end

    def create(sobject, records, options ={})
      do_operation('insert', sobject, records, options)
    end

    def delete(sobject, records, options ={})
      do_operation('delete', sobject, records, options)
    end

    def query(sobject, query, options ={})
      do_operation('query', sobject, query, options)
    end

    def counters
      {
        http_get: @connection.counters[:get],
        http_post: @connection.counters[:post],
        upsert: get_counters[:upsert],
        update: get_counters[:update],
        create: get_counters[:create],
        delete: get_counters[:delete],
        query: get_counters[:query]
      }
    end


    ##
    # Allows you to attach a listener that accepts the created job (which has a useful #job_id field).  This is useful
    # for recording a job ID persistently before you begin batch work (i.e. start modifying the salesforce database),
    # so if the load process you are writing needs to recover, it can be aware of previous jobs it started and wait
    # for them to finish.
    def on_job_created(&block)
      @listeners[:job_created] << block
    end

    def job_from_id(job_id)
      SalesforceBulkApi::Job.new(job_id: job_id, connection: @connection)
    end

    def do_operation(operation, sobject, records, options = {})
      count operation.to_sym

      get_response = options.fetch(:get_response, false)
      send_nulls = options.fetch(:send_nulls, false)
      no_null_list = options.fetch(:no_null_list, [])
      batch_size = options.fetch(:batch_size, DEFAULT_BATCH_SIZE)
      timeout = options.fetch(:timeout, DEFAULT_TIMEOUT)
      external_key = options.fetch(:external_key) if operation == 'upsert'

      job = SalesforceBulkApi::Job.new(
        operation: operation,
        sobject: sobject,
        records: records,
        external_key: external_key,
        connection: @connection,
        serial: options.fetch(:serial, false)
      )

      job.create_job(batch_size, send_nulls, no_null_list)
      @listeners[:job_created].each {|callback| callback.call(job)}

      if operation == "query"
        job.add_query()
      else
        job.add_batches()
      end

      response = job.close_job
      response.merge!({'batches' => job.get_job_result(get_response, timeout)}) if get_response == true

      return response
    end

    private
    def get_counters
      @counters ||= Hash.new(0)
    end

    def count(name)
      get_counters[name] += 1
    end

  end
end

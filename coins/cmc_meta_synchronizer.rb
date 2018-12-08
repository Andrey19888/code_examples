module Coins
  class CmcMetaSynchronizer
    CONFLICT_KEY_COLUMNS = %i[coin_id].freeze

    # [NOTE] we can update only attributes which cmc returns,
    # all other we can't touch to prevent nullify them
    CONFLICT_UPDATE_COLUMNS = %i[
      cmc_category
      cmc_slug
      cmc_logo_url
      website_url
      announcement_url
      twitter_url
      updated_at
    ].freeze

    class InvalidMeta < StandardError
      def initialize(params:, errors:)
        super("errors: #{errors.inspect}; params: #{params.inspect}")
      end
    end

    def perform
      mapping = target_coins
      return if mapping.blank?

      DB[:coins]
        .left_join(:coins_meta, coin_id: :id)
        .where do
          (Sequel[:coins_meta][:id] =~ nil) | (Sequel[:coins_meta][:updated_at] <= 3.day.ago)
        end
        .exclude(cmc_id: nil)
        .select_hash(Sequel[:coins][:cmc_id], Sequel[:coins][:id])

      cmc_ids = mapping.keys

      # { cmc_id => meta (Hash) }
      coins_meta = CmcMetaFetcher.new.fetch(cmc_ids)
      attributes = build_attributes(raw_meta: coins_meta, mapping: mapping)

      current_timestamp = Time.current
      attributes.each do |coin_attributes|
        coin_attributes.merge!(created_at: current_timestamp, updated_at: current_timestamp)
      end

      save(attributes)
    end

    private

    # returns target coins to sync meta: { cmc_id => internal_coin_id, ... }
    def target_coins(force_include_older_than: 1.month.ago)
      DB[:coins]
        .left_join(:coins_meta, coin_id: :id)
        .where { (Sequel[:coins_meta][:id] =~ nil) | (Sequel[:coins_meta][:updated_at] <= force_include_older_than) }
        .exclude(cmc_id: nil)
        .select_hash(Sequel[:coins][:cmc_id], Sequel[:coins][:id])
    end

    def build_attributes(raw_meta:, mapping:)
      raw_meta.map do |cmc_id, meta|
        urls = meta.fetch('urls')

        params = {
          coin_id: mapping.fetch(cmc_id),

          cmc_category: meta.fetch('category'),
          cmc_slug:     meta.fetch('slug'),
          cmc_logo_url: meta.fetch('logo'),

          website_url:      urls.fetch('website').first,
          announcement_url: urls.fetch('announcement').first,
          twitter_url:      urls.fetch('twitter').first
        }

        operation = BuildCmcMeta.new(params)
        result = operation.perform

        if result.success?
          result.data
        else
          raise InvalidMeta.new(params: params, errors: result.errors)
        end
      end
    end

    def save(attributes = [])
      columns_to_update = CONFLICT_UPDATE_COLUMNS.map { |column| [column, Sequel[:excluded][column.to_sym]] }.to_h

      DB[:coins_meta].insert_conflict(
        target: CONFLICT_KEY_COLUMNS,
        update: columns_to_update,
        update_where: Sequel[:excluded][:updated_at] > Sequel[:coins_meta][:updated_at]
      ).multi_insert(
        attributes
      )
    end
  end
end

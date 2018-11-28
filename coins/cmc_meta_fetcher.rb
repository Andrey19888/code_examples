module Coins
  class CmcMetaFetcher
    ENDPOINT = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/info'.freeze
    BATCH_SIZE = 100
    IDS_SEPARATOR = ','.freeze

    def fetch(*cmc_ids)
      all_ids = cmc_ids.flatten.compact.uniq.sort

      data = {}

      all_ids.in_groups_of(BATCH_SIZE, false).each do |batch_ids|
        group_data = fetch_data(batch_ids)
        group_data.transform_keys! { |id| Integer(id) }
        data.merge!(group_data)
      end

      data
    end

    private

    def fetch_data(ids)
      params = { id: ids.join(IDS_SEPARATOR) }
      CmcClient.new.get(ENDPOINT, params)
    end
  end
end

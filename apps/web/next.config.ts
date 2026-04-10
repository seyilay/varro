import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  experimental: {
    // Enable React 19 features
  },
  transpilePackages: ['@varro/database', '@varro/shared-types'],
}

export default nextConfig

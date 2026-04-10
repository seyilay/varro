import 'dotenv/config'
import { Worker, Queue } from 'bullmq'
import { createClient } from '@supabase/supabase-js'
import pino from 'pino'

const logger = pino({ level: process.env.LOG_LEVEL || 'info' })

// Redis connection (Upstash)
const redisConnection = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD,
  tls: process.env.REDIS_TLS === 'true' ? {} : undefined,
}

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

// Queue names
export const QUEUES = {
  INGESTION: 'ingestion',
  BSEE_WELLS: 'bsee-wells',
  BOEM_LEASES: 'boem-leases',
  BOEM_PA_COSTS: 'boem-pa-costs',
} as const

logger.info('Varro worker starting...')

// Workers will be registered here as they are built
// TODO: register job handlers for each queue

logger.info('Worker running. Waiting for jobs...')

#!/bin/bash
# backup.sh
docker-compose exec postgres pg_dump -U postgres lora_db > backup_$(date +%Y-%m-%d_%H-%M-%S).sql
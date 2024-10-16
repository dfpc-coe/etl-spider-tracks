import { Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection } from 'geojson';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';

const Env = Type.Object({
    'Username': Type.String(),
    'Password': Type.String(),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Env;
        } else {
            return Type.Object({
            });
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Env);

        const res = await fetch('https://apigw.spidertracks.io/go/aff/feed', {
            method: 'POST',
            headers: {
                Authorization: `Basic ${btoa(env.Username + ':' + env.Password)}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                type: 'dataRequest',
                dataCenter: [{
                    affVer: "json 1.0",
                    name: "AFF",
                    reqTime: new Date().toISOString()
                }],
                msgRequest: [{
                    to: 'spidertracks',
                    from: `CloudTAK-${env.Username}`,
                    msgType: 'dataRequest',
                    dataCtrlTime: new Date().toISOString()
                }]
            })
        })

        if (!res.ok) throw new Error(await res.text());


        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        }

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}


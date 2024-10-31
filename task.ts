import { Type, TSchema } from '@sinclair/typebox';
import moment from 'moment-timezone';
import { FeatureCollection, Feature } from 'geojson';
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
                ctrId: Type.String(),
                esn: Type.String(),
                fix: Type.String(),
                hdop: Type.Integer(),
                posTime: Type.String({
                    format: 'date-time'
                }),
                dataCtrTime: Type.String({
                    format: 'date-time'
                }),
                src: Type.String(),
                unitId: Type.String(),
                trackId: Type.String()
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
                    dataCtrTime: moment().subtract(1, 'hour').toISOString()
                }]
            })
        })

        if (!res.ok) throw new Error(await res.text());

        const body = await res.typed(Type.Object({
            type: Type.Literal('FeatureCollection'),
            dataInfo: Type.Array(Type.Object({
                  affVer: Type.String(),
                  provider: Type.String(),
                  rptTime: Type.String(),
                  reqTime: Type.String(),
                  sysId: Type.String()
            })),
            features: Type.Array(Type.Object({
                type: Type.Literal('Feature'),
                properties: Type.Any(),
                geometry: Type.Object({
                    type: Type.Literal('Point'),
                    coordinates: Type.Array(Type.Number())
                })
            }))
        }))

        

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: body.features.map((feat: Feature) => {
                return {
                    type: 'Feature',
                    properties: {
                        course: feat.properties.cog,
                        speed: feat.properties.speed,
                        metadata: {
                            ctrId: feat.properties.ctrId,
                            esn: feat.properties.esn,
                            fix: feat.properties.fix,
                            hdop: feat.properties.hdop,
                            posTime: feat.properties.postime,
                            dataCtrTime: feat.properties.dataCtrTime,
                            src: feat.properties.src,
                            unitId: feat.properties.unitId,
                            trackId: feat.properties.trackId
                        }
                    },
                    geometry: feat.geometry
                }
            })
        }

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}


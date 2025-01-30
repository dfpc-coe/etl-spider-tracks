import { Static, Type, TSchema } from '@sinclair/typebox';
import moment from 'moment-timezone';
import type { Event } from '@tak-ps/etl';
import { Feature } from 'geojson';
import ETL, { SchemaType, handler as internal, local, InputFeatureCollection, InputFeature, DataFlowType, InvocationType } from '@tak-ps/etl';
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
    static name = 'etl-spider-tracks';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
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
        } else {
            return Type.Object({});
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

        const latest: Map<string | number, Static<typeof InputFeature>> = new Map();
        body.features.forEach((feat: Feature) => {
            const processed: Static<typeof InputFeature> = {
                id: feat.properties.unitId,
                type: 'Feature',
                properties: {
                    course: feat.properties.cog,
                    speed: feat.properties.speed,
                    metadata: {
                        ctrId: feat.properties.ctrId,
                        esn: feat.properties.esn,
                        fix: feat.properties.fix,
                        hdop: feat.properties.hdop,
                        posTime: feat.properties.posTime,
                        dataCtrTime: feat.properties.dataCtrTime,
                        src: feat.properties.src,
                        unitId: feat.properties.unitId,
                        trackId: feat.properties.trackId
                    }
                },
                // @ts-expect-error Geometry Type
                geometry: feat.geometry
            }

            const previous = latest.get(processed.id);
            if (!previous) {
                latest.set(processed.id, processed);
            // @ts-expect-error Untyped
            } else if (new Date(previous.properties.metadata.posTime) < new Date(processed.properties.metadata.posTime)) {
                latest.set(processed.id, processed)
            }
        });

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: Array.from(latest.values())
        }

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}


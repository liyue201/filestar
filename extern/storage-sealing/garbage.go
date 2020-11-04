package sealing

import (
	"context"
	"errors"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
)

func (m *Sealing) pledgeSector(ctx context.Context, sectorID abi.SectorID, existingPieceSizes []abi.UnpaddedPieceSize, sizes ...abi.UnpaddedPieceSize) ([]abi.PieceInfo, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	out := make([]abi.PieceInfo, len(sizes))
	for i, size := range sizes {
		ppi, err := m.sealer.AddPiece(ctx, sectorID, existingPieceSizes, size, NewNullReader(size))
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = ppi
	}

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() > cfg.MaxSealingSectors {
			return xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}
	if !m.sealer.CanAddPiece() {
		return errors.New("too many sectors sealing, out off task limit")
	}

	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		size := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

		sid, err := m.sc.Next()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}
		err = m.sealer.NewSector(ctx, m.minerSector(sid))
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		pieces, err := m.pledgeSector(ctx, m.minerSector(sid), []abi.UnpaddedPieceSize{}, size)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		ps := make([]Piece, len(pieces))
		for idx := range ps {
			ps[idx] = Piece{
				Piece:    pieces[idx],
				DealInfo: nil,
			}
		}

		if err := m.newSectorCC(sid, ps); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}

func (m *Sealing) RunPledgeSectors(ctx context.Context) error {

	ticker := time.NewTicker(time.Second)
	count := uint64(0)
	for {
		select {
		case <-ticker.C:
			count++
			cfg, err := m.getConfig()
			if err != nil {
				log.Infof("getConfig: %v", err)
				break
			}
			log.Infof("AutoPledgeSectorInterval=%v, count=%v", cfg.AutoPledgeSectorInterval, count)

			if cfg.AutoPledgeSectorInterval == 0 {
				count = 0
				break
			}
			if count < cfg.AutoPledgeSectorInterval {
				break
			}
			m.PledgeSector()
			count = 0
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}
